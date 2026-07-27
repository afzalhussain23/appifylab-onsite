import express, {
  type NextFunction,
  type Request,
  type Response,
} from "express";
import { Pool } from "pg";
import { randomUUID } from "node:crypto";
import {
  redis,
  enrollmentRateLimit,
  qstash,
  qstashReceiver,
  getRemainingSeatsKey,
  getEnrollmentQueueName,
  getEnrollmentRequestKey,
  getCancellationRequestKey,
  updateCancellationStatus,
} from "./upstash";

const databaseUrl = Bun.env.DATABASE_URL;
const portValue = Bun.env.PORT ?? "3000";

if (!databaseUrl) {
  throw new Error("DATABASE_URL is not defined in the .env file");
}

const port = Number(portValue);

if (!Number.isInteger(port) || port <= 0) {
  throw new Error(`Invalid PORT value: ${portValue}`);
}

const pool = new Pool({
  connectionString: databaseUrl,
  max: 10,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
});

interface CancelEnrollmentJob {
  requestId: string;
  schoolId: number;
  classId: number;
  studentId: number;
}

interface BookingRow {
  id: number;
  school_id: number;
  live_class_id: number;
  student_id: number;
  status: "confirmed" | "waitlisted" | "cancelled" | "attended" | "no_show";
  booked_at: number;
  cancelled_at: number | null;
  deleted_at: number | null;
}

const app = express();

app.use(express.json());

app.post(
  "/classes/:id/enroll",
  async (
    request: Request,
    response: Response,
    next: NextFunction,
  ): Promise<void> => {
    const classId = Number(request.params.id);
    const studentId = request.user?.id;
    const schoolId = request.user?.schoolId;

    if (!Number.isSafeInteger(classId) || classId <= 0) {
      response.status(400).json({
        success: false,
        message: "Invalid class ID.",
      });
      return;
    }

    if (!studentId || !schoolId) {
      response.status(401).json({
        success: false,
        message: "Authentication required.",
      });
      return;
    }

    try {
      const rateLimitResult = await enrollmentRateLimit.limit(
        `${schoolId}:${studentId}`,
      );

      if (!rateLimitResult.success) {
        response.status(429).json({
          success: false,
          message: "Too many enrollment attempts. Try again shortly.",
          retryAt: rateLimitResult.reset,
        });
        return;
      }

      const classResult = await pool.query(
        `
           SELECT id
           FROM live_classes
           WHERE id = $1
             AND school_id = $2
             AND status = 'scheduled'
             AND deleted_at IS NULL
           LIMIT 1
         `,
        [classId, schoolId],
      );

      if (classResult.rowCount === 0) {
        response.status(404).json({
          success: false,
          message: "Class not found or unavailable.",
        });
        return;
      }

      const requestId = randomUUID();

      const enrollKey = getEnrollmentRequestKey(schoolId, classId, studentId);
      // check existance first

      await redis.hset(enrollKey, {
        status: "queued",
        createdAt: Math.floor(Date.now() / 1000),
      });

      await redis.expire(enrollKey, 60 * 60 * 24);

      const queue = qstash.queue({
        queueName: getEnrollmentQueueName(schoolId, classId),
      });

      const queuedMessage = await queue.enqueueJSON({
        url: `${process.env.PUBLIC_API_URL}` + "/internal/enroll/process",

        body: {
          requestId,
          schoolId,
          classId,
          studentId,
        },

        retries: 3,
      });

      response.status(202).json({
        success: true,
        message: "Enrollment request has been queued.",
        data: {
          requestId,
          queueMessageId: queuedMessage.messageId,
          status: "queued",
        },
      });
    } catch (error: unknown) {
      next(error);
    }
  },
);

app.post(
  "/internal/enroll/process",
  async (request: Request, response: Response): Promise<void> => {
    const signature = request.header("upstash-signature");
    const rawBody = request.rawBody;

    if (!signature || !rawBody) {
      response.status(401).json({
        success: false,
        message: "Missing QStash signature.",
      });
      return;
    }

    try {
      const valid = await qstashReceiver.verify({
        signature,
        body: rawBody,
      });

      if (!valid) {
        response.status(401).json({
          success: false,
          message: "Invalid QStash signature.",
        });
        return;
      }
    } catch {
      response.status(401).json({
        success: false,
        message: "Invalid QStash signature.",
      });
      return;
    }

    const { requestId, schoolId, classId, studentId } = request.body;

    if (
      typeof requestId !== "string" ||
      !Number.isSafeInteger(schoolId) ||
      !Number.isSafeInteger(classId) ||
      !Number.isSafeInteger(studentId)
    ) {
      // Return 200 so QStash does not retry a permanently invalid job.
      response.status(200).json({
        success: false,
        status: "invalid_job",
      });
      return;
    }

    const statusKey = getEnrollmentRequestKey(schoolId, classId, studentId):
    const seatKey = getRemainingSeatsKey(schoolId, classId);

    /*
     * Request-level idempotency for repeated QStash delivery.
     */
    try {
      const previous = await redis.hgetall(statusKey);

      if (
        previous.status === "confirmed" ||
        previous.status === "waitlisted" ||
        previous.status === "class_unavailable"
      ) {
        response.status(200).json({
          success: previous.status !== "class_unavailable",
          status: previous.status,
          enrollmentId: previous.enrollmentId || undefined,
          remainingSeats:
            previous.remainingSeats !== undefined
              ? Number(previous.remainingSeats)
              : undefined,
        });
        return;
      }

      await redis.hset(statusKey, {
        status: "processing",
        processedAt: Math.floor(Date.now() / 1000),
      });
    } catch (error) {
      console.error("Failed to read or update request status", {
        requestId,
        error,
      });
    }

    const client = await pool.connect();

    let enrollment: BookingRow | undefined;
    let enrollmentStatus: "confirmed" | "waitlisted";
    let remainingSeats = 0;

    try {
      await client.query("BEGIN");

      const classResult = await client.query<{
        id: number;
        seat_capacity: number;
      }>(
        `
          SELECT
            id,
            seat_capacity
          FROM live_classes
          WHERE id = $1
            AND school_id = $2
            AND status = 'scheduled'
            AND deleted_at IS NULL
          LIMIT 1
          FOR UPDATE
        `,
        [classId, schoolId],
      );

      if (classResult.rows.length === 0) {
        await client.query("ROLLBACK");

        try {
          await redis.hset(statusKey, {
            status: "class_unavailable",
            completedAt: Math.floor(Date.now() / 1000),
          });
        } catch {
          // PostgreSQL remains authoritative.
        }

        response.status(200).json({
          success: false,
          status: "class_unavailable",
        });
        return;
      }

      const seatCapacity = Number(classResult.rows[0].seat_capacity);

      /*
       * Read the latest booking while holding the class lock.
       */
      const existingResult = await client.query<BookingRow>(
        `
          SELECT
            id,
            school_id,
            live_class_id,
            student_id,
            status,
            booked_at,
            cancelled_at,
            deleted_at
          FROM class_bookings
          WHERE school_id = $1
            AND live_class_id = $2
            AND student_id = $3
            AND deleted_at IS NULL
          ORDER BY id DESC
          LIMIT 1
          FOR UPDATE
        `,
        [schoolId, classId, studentId],
      );

      const existing = existingResult.rows[0];

      if (
        existing?.status === "confirmed" ||
        existing?.status === "waitlisted"
      ) {
        await client.query("COMMIT");

        try {
          await redis.hset(statusKey, {
            status: existing.status,
            enrollmentId: existing.id,
            completedAt: Math.floor(Date.now() / 1000),
          });
        } catch {
          // Do not fail an already successful database operation.
        }

        response.status(200).json({
          success: true,
          status: existing.status,
          enrollmentId: existing.id,
        });
        return;
      }

      const countResult = await client.query<{
        confirmed_count: number;
      }>(
        `
          SELECT COUNT(*)::INTEGER AS confirmed_count
          FROM class_bookings
          WHERE school_id = $1
            AND live_class_id = $2
            AND status = 'confirmed'
            AND deleted_at IS NULL
        `,
        [schoolId, classId],
      );

      const confirmedCount = Number(countResult.rows[0].confirmed_count);

      enrollmentStatus =
        confirmedCount < seatCapacity ? "confirmed" : "waitlisted";

      if (existing?.status === "cancelled") {
        /*
         * Reuse the cancelled row. This avoids depending on the exact
         * uniqueness constraint currently present in the database.
         *
         * Adjust booked_at assignment if booked_at is a timestamp rather
         * than an epoch integer.
         */
        const updateResult = await client.query<BookingRow>(
          `
            UPDATE class_bookings
            SET
              status = $2,
              cancelled_at = NULL,
              booked_at =
                FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::BIGINT
            WHERE id = $1
            RETURNING
              id,
              school_id,
              live_class_id,
              student_id,
              status,
              booked_at,
              cancelled_at,
              deleted_at
          `,
          [existing.id, enrollmentStatus],
        );

        enrollment = updateResult.rows[0];
      } else {
        const insertResult = await client.query<BookingRow>(
          `
            INSERT INTO class_bookings (
              school_id,
              live_class_id,
              student_id,
              status
            )
            VALUES ($1, $2, $3, $4)
            RETURNING
              id,
              school_id,
              live_class_id,
              student_id,
              status,
              booked_at,
              cancelled_at,
              deleted_at
          `,
          [schoolId, classId, studentId, enrollmentStatus],
        );

        enrollment = insertResult.rows[0];
      }

      const newConfirmedCount =
        confirmedCount + (enrollmentStatus === "confirmed" ? 1 : 0);

      remainingSeats = Math.max(seatCapacity - newConfirmedCount, 0);

      await client.query("COMMIT");
    } catch (error: unknown) {
      try {
        await client.query("ROLLBACK");
      } catch (rollbackError) {
        console.error("Enrollment rollback failed", rollbackError);
      }

      /*
       * Handles duplicate delivery or an ambiguous COMMIT result.
       * Do not blindly treat every 23505 as success; verify the row.
       */
      try {
        const existingResult = await pool.query<BookingRow>(
          `
            SELECT
              id,
              school_id,
              live_class_id,
              student_id,
              status,
              booked_at,
              cancelled_at,
              deleted_at
            FROM class_bookings
            WHERE school_id = $1
              AND live_class_id = $2
              AND student_id = $3
              AND status IN ('confirmed', 'waitlisted')
              AND deleted_at IS NULL
            ORDER BY id DESC
            LIMIT 1
          `,
          [schoolId, classId, studentId],
        );

        const existing = existingResult.rows[0];

        if (existing) {
          try {
            await redis.hset(statusKey, {
              status: existing.status,
              enrollmentId: existing.id,
              completedAt: Math.floor(Date.now() / 1000),
            });
          } catch {
            // PostgreSQL remains authoritative.
          }

          response.status(200).json({
            success: true,
            status: existing.status,
            enrollmentId: existing.id,
          });
          return;
        }
      } catch (verificationError) {
        console.error("Failed to verify enrollment after error", {
          requestId,
          verificationError,
        });
      }

      try {
        await redis.hset(statusKey, {
          status: "retrying",
          error:
            error instanceof Error ? error.message : "Unknown processing error",
        });
      } catch {
        // Status tracking is not authoritative.
      }

      response.status(500).json({
        success: false,
        message: "Enrollment processing failed.",
      });
      return;
    } finally {
      client.release();
    }

    /*
     * Database committed successfully.
     * Redis failures must not cause QStash to retry the enrollment.
     */
    try {
      await Promise.all([
        redis.del(seatKey),
        redis.hset(statusKey, {
          status: enrollmentStatus,
          enrollmentId: enrollment!.id,
          remainingSeats,
          completedAt: Math.floor(Date.now() / 1000),
        }),
      ]);
    } catch (error) {
      console.error("Post-enrollment cache update failed", {
        requestId,
        error,
      });
    }

    response.status(200).json({
      success: true,
      status: enrollmentStatus,
      message:
        enrollmentStatus === "confirmed"
          ? "Enrollment confirmed."
          : "Class is full. You have been added to the waitlist.",
      remainingSeats,
      data: enrollment,
    });
  },
);

app.delete(
  "/classes/:id/enroll",
  async (
    request: Request,
    response: Response,
    next: NextFunction,
  ): Promise<void> => {
    const classId = Number(request.params.id);
    const studentId = request.user?.id;
    const schoolId = request.user?.schoolId;

    if (!Number.isSafeInteger(classId) || classId <= 0) {
      response.status(400).json({
        success: false,
        message: "Invalid class ID.",
      });
      return;
    }

    if (!studentId || !schoolId) {
      response.status(401).json({
        success: false,
        message: "Authentication required.",
      });
      return;
    }

    try {
      const rateLimitResult = await enrollmentRateLimit.limit(
        `cancel:${schoolId}:${studentId}`,
      );

      if (!rateLimitResult.success) {
        response.status(429).json({
          success: false,
          message: "Too many cancellation attempts. Try again shortly.",
          retryAt: rateLimitResult.reset,
        });
        return;
      }

      const requestId = randomUUID();

      const queue = qstash.queue({
        queueName: getEnrollmentQueueName(schoolId, classId),
      });

      const statusKey = getCancellationRequestKey(schoolId, classId, studentId);

      await redis.hset(statusKey, {
        requestId,
        operation: "cancel_enrollment",
        schoolId,
        classId,
        studentId,
        status: "queued",
        createdAt: Math.floor(Date.now() / 1000),
      });

      await redis.expire(statusKey, 60 * 60 * 24);

      const message = await queue.enqueueJSON({
        url: `${process.env.PUBLIC_API_URL}` + "/internal/enroll/cancel",

        body: {
          requestId,
          schoolId,
          classId,
          studentId,
        } satisfies CancelEnrollmentJob,

        retries: 3,
      });

      response.status(202).json({
        success: true,
        message: "Cancellation request has been queued.",
        data: {
          requestId,
          queueMessageId: message.messageId,
          status: "queued",
        },
      });
    } catch (error: unknown) {
      next(error);
    }
  },
);

app.post(
  "/internal/enroll/cancel",
  async (request: Request, response: Response): Promise<void> => {
    const signature = request.header("upstash-signature");
    const rawBody = request.rawBody;

    if (!signature || !rawBody) {
      response.status(401).json({
        success: false,
        message: "Missing QStash signature.",
      });
      return;
    }

    try {
      const valid = await qstashReceiver.verify({
        signature,
        body: rawBody,
      });

      if (!valid) {
        response.status(401).json({
          success: false,
          message: "Invalid QStash signature.",
        });
        return;
      }
    } catch {
      response.status(401).json({
        success: false,
        message: "Invalid QStash signature.",
      });
      return;
    }

    const { requestId, schoolId, classId, studentId } =
      request.body as CancelEnrollmentJob;

    if (
      !requestId ||
      !Number.isSafeInteger(schoolId) ||
      schoolId <= 0 ||
      !Number.isSafeInteger(classId) ||
      classId <= 0 ||
      !Number.isSafeInteger(studentId) ||
      studentId <= 0
    ) {
      // 200 intentionally prevents retries for a permanently invalid job.
      response.status(200).json({
        success: false,
        status: "invalid_job",
      });
      return;
    }

    const statusKey = getCancellationRequestKey(schoolId, classId, studentId);
    const seatKey = getRemainingSeatsKey(schoolId, classId);

    await updateCancellationStatus(statusKey, {
      status: "processing",
      processedAt: Math.floor(Date.now() / 1000),
    });

    let client;

    try {
      client = await pool.connect();
      await client.query("BEGIN");

      const classResult = await client.query<{
        id: number;
        status: string;
        seat_capacity: number;
      }>(
        `
          SELECT id, status, seat_capacity
          FROM live_classes
          WHERE id = $1
            AND school_id = $2
            AND deleted_at IS NULL
          FOR UPDATE
        `,
        [classId, schoolId],
      );

      if (classResult.rowCount === 0) {
        await client.query("ROLLBACK");

        await updateCancellationStatus(statusKey, {
          status: "class_not_found",
          completedAt: Math.floor(Date.now() / 1000),
        });

        response.status(200).json({
          success: false,
          status: "class_not_found",
        });
        return;
      }

      const liveClass = classResult.rows[0];

      const bookingResult = await client.query<BookingRow>(
        `
          SELECT
            id,
            school_id,
            live_class_id,
            student_id,
            status,
            booked_at,
            cancelled_at,
            deleted_at
          FROM class_bookings
          WHERE
            AND school_id = $2
            AND live_class_id = $3
            AND student_id = $4
            AND deleted_at IS NULL
          FOR UPDATE
        `,
        [schoolId, classId, studentId],
      );

      if (bookingResult.rowCount === 0) {
        await client.query("ROLLBACK");

        await updateCancellationStatus(statusKey, {
          status: "not_enrolled",
          completedAt: Math.floor(Date.now() / 1000),
        });

        response.status(200).json({
          success: false,
          status: "not_enrolled",
        });
        return;
      }

      const booking = bookingResult.rows[0];

      if (booking.status === "cancelled") {
        await client.query("ROLLBACK");

        await updateCancellationStatus(statusKey, {
          status: "already_cancelled",
          cancelledEnrollmentId: booking.id,
          completedAt: Math.floor(Date.now() / 1000),
        });

        response.status(200).json({
          success: true,
          status: "already_cancelled",
          data: {
            cancelledEnrollmentId: booking.id,
          },
        });
        return;
      }

      if (booking.status !== "confirmed" && booking.status !== "waitlisted") {
        await client.query("ROLLBACK");

        await updateCancellationStatus(statusKey, {
          status: "not_cancellable",
          completedAt: Math.floor(Date.now() / 1000),
        });

        response.status(200).json({
          success: false,
          status: "not_cancellable",
        });
        return;
      }

      await client.query(
        `
          UPDATE class_bookings
          SET
            status = 'cancelled',
            cancelled_at =
              FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::BIGINT
          WHERE id = $1
        `,
        [booking.id],
      );

      let promotedBooking: BookingRow | null = null;

      if (booking.status === "confirmed" && liveClass.status === "scheduled") {
        const waitlistResult = await client.query<BookingRow>(
          `
            SELECT
              id,
              school_id,
              live_class_id,
              student_id,
              status,
              booked_at,
              cancelled_at,
              deleted_at
            FROM class_bookings
            WHERE school_id = $1
              AND live_class_id = $2
              AND status = 'waitlisted'
              AND deleted_at IS NULL
            ORDER BY booked_at ASC, id ASC
            LIMIT 1
            FOR UPDATE
          `,
          [schoolId, classId],
        );

        if (waitlistResult.rowCount > 0) {
          const promotionResult = await client.query<BookingRow>(
            `
              UPDATE class_bookings
              SET
                status = 'confirmed',
                cancelled_at = NULL
              WHERE id = $1
                AND status = 'waitlisted'
              RETURNING
                id,
                school_id,
                live_class_id,
                student_id,
                status,
                booked_at,
                cancelled_at,
                deleted_at
            `,
            [waitlistResult.rows[0].id],
          );

          promotedBooking = promotionResult.rows[0] ?? null;
        }
      }

      const seatsResult = await client.query<{
        remaining_seats: number;
      }>(
        `
          SELECT GREATEST(
            live_class.seat_capacity - COUNT(booking.id),
            0
          )::INTEGER AS remaining_seats
          FROM live_classes AS live_class
          LEFT JOIN class_bookings AS booking
            ON booking.school_id = live_class.school_id
           AND booking.live_class_id = live_class.id
           AND booking.status = 'confirmed'
           AND booking.deleted_at IS NULL
          WHERE live_class.id = $1
            AND live_class.school_id = $2
          GROUP BY live_class.seat_capacity
        `,
        [classId, schoolId],
      );

      const remainingSeats = seatsResult.rows[0]?.remaining_seats ?? 0;

      await client.query("COMMIT");

      /*
       * PostgreSQL has committed. Redis is now only a cache.
       * Never rollback or compensate after this point.
       */
      try {
        await redis.set(seatKey, remainingSeats);
      } catch (error) {
        console.error("Failed to synchronize remaining seats", {
          requestId,
          schoolId,
          classId,
          remainingSeats,
          error,
        });
      }

      const finalStatus = promotedBooking
        ? "cancelled_and_promoted"
        : "cancelled";

      await updateCancellationStatus(statusKey, {
        status: finalStatus,
        cancelledEnrollmentId: booking.id,
        promotedEnrollmentId: promotedBooking?.id ?? "",
        promotedStudentId: promotedBooking?.student_id ?? "",
        remainingSeats,
        completedAt: Math.floor(Date.now() / 1000),
      });

      response.status(200).json({
        success: true,
        status: finalStatus,
        data: {
          cancelledEnrollmentId: booking.id,
          promotedEnrollment: promotedBooking,
          remainingSeats,
        },
      });
    } catch (error: unknown) {
      if (client) {
        try {
          await client.query("ROLLBACK");
        } catch (rollbackError) {
          console.error("Cancellation rollback failed", rollbackError);
        }
      }

      await updateCancellationStatus(statusKey, {
        status: "retrying",
        error:
          error instanceof Error ? error.message : "Unknown cancellation error",
      });

      response.status(500).json({
        success: false,
        message: "Cancellation processing failed.",
      });
    } finally {
      client?.release();
    }
  },
);

async function startServer(): Promise<void> {
  try {
    await pool.query("SELECT 1");

    app.listen(port, () => {
      console.log(`Server running at http://localhost:${port}`);
      console.log(`Enrollments: http://localhost:${port}/enrollments`);
    });
  } catch (error: unknown) {
    const message =
      error instanceof Error
        ? error.message
        : "Unknown database connection error";

    console.error(`Could not connect to PostgreSQL: ${message}`);
    await pool.end();
    process.exit(1);
  }
}

await startServer();
