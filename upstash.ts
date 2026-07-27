import { Redis } from "@upstash/redis";
import { Ratelimit } from "@upstash/ratelimit";
import { Client, Receiver } from "@upstash/qstash";

export const redis = Redis.fromEnv();

export const qstash = new Client({
  token: process.env.QSTASH_TOKEN!,
});

export const qstashReceiver = new Receiver({
  currentSigningKey: process.env.QSTASH_CURRENT_SIGNING_KEY!,
  nextSigningKey: process.env.QSTASH_NEXT_SIGNING_KEY!,
});

export const enrollmentRateLimit = new Ratelimit({
  redis,
  limiter: Ratelimit.slidingWindow(5, "10 s"),
  prefix: "ratelimit:enrollment",
});

export const getEnrollmentQueueName(schoolId: number, classId: number): string {
  return `class-${schoolId}-${classId}-enrollments`;
}

export const getRemainingSeatsKey(schoolId: number, classId: number): string {
  return `class:${schoolId}:${classId}:remaining-seats`;
}

export const getEnrollmentRequestKey(schoolId, classId, studentId): string {
  return `school:${schoolId}:class:${classId}:student:${studentId}`;
}

export const getCancellationRequestKey(schoolId, classId, studentId): string {
  return `cancellation-request:school:${schoolId}:class:${classId}:student:${studentId}`;
}

export const updateCancellationStatus(
  statusKey: string,
  values: Record<string, string | number>,
): Promise<void> {
  try {
    await redis.hset(statusKey, values);
  } catch (error) {
    console.error("Failed to update cancellation status", {
      statusKey,
      error,
    });
  }
}
