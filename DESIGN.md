# DESIGN.md

## Purpose

This service manages asynchronous enrollment and cancellation for scheduled live classes. It uses PostgreSQL as the source of truth, Redis for fast seat accounting and request status, and Upstash QStash for serialized background processing.

## Architecture

- **Express API** accepts enrollment and cancellation requests.
- **QStash queues** isolate work by `schoolId` and `classId`, reducing concurrent updates for the same class.
- **PostgreSQL** stores classes and bookings.
- **Redis** stores remaining-seat counters and short-lived request status records.
- **Rate limiting** is applied per school and student.

## Enrollment Workflow

### 1. Receive Enrollment Request

**Endpoint:** `POST /classes/:id/enroll`

The API:

- Validates the class ID.
- Reads the student ID and school ID from the authenticated user.
- Applies a per-school and per-student rate limit.
- Checks that the class:
  - Belongs to the same school.
  - Has `scheduled` status.
  - Has not been deleted.
- Generates a unique request ID.
- Stores the request in Redis with status `queued`.
- Sends an enrollment job to the queue for that school and class.
- Returns **`202 Accepted`** with the request ID.

---

### 2. Process Enrollment Job

**Endpoint:** `POST /internal/enroll/process`

The worker:

- Verifies the QStash signature.
- Validates the enrollment job data.
- Changes the Redis request status to `processing`.
- Starts a PostgreSQL transaction.
- Locks the target `live_classes` row using `FOR UPDATE`.
  - This serializes concurrent enrollment decisions for the same class.
- Confirms that the class:
  - Belongs to the correct school.
  - Is still `scheduled`.
  - Has not been deleted.
- If the class is unavailable:
  - Rolls back the transaction.
  - Finishes with `class_unavailable`.
- Checks whether the student already has an active `confirmed` or `waitlisted` booking for the class.
- If a booking exists:
  - Rolls back the transaction.
  - Finishes with the existing enrollment status.
- Counts the active `confirmed` bookings for the class.
- Compares the confirmed booking count with `live_classes.seat_capacity`.
- Selects the booking status:
  - If capacity remains, selects `confirmed`.
  - If capacity has been reached, selects `waitlisted`.
- Inserts the booking into PostgreSQL with the selected status.
- Commits the PostgreSQL transaction.
- If the student is `waitlisted`:
  - Restores any seat previously reserved in Redis because a waitlisted booking does not consume a seat.
- Changes the Redis request status to either:
  - `confirmed`; or
  - `waitlisted`.
- Returns the completed booking and its final enrollment status.

## Cancellation Workflow

### 1. Receive Cancellation Request

**Endpoint:** `DELETE /classes/:id/enroll`

The API:

- Validates the class ID.
- Reads the student ID and school ID from the authenticated user.
- Applies a per-school and per-student cancellation rate limit.
- Generates a unique request ID.
- Stores the cancellation request in Redis with status `queued`.
- Sends a cancellation job to the queue for that school and class.
- Returns **`202 Accepted`** with the request ID.

---

### 2. Process Cancellation Job

**Endpoint:** `POST /internal/enroll/cancel`

The worker:

- Verifies the QStash signature.
- Validates the cancellation job data.
- Changes the Redis request status to `processing`.
- Starts a PostgreSQL transaction.
- Locks the target `live_classes` row using `FOR UPDATE`.
  - This serializes enrollment, cancellation, and waitlist promotion decisions for the same class.
- Confirms that the class:
  - Belongs to the correct school.
  - Has not been deleted.
- Finds and locks the student’s active `confirmed` or `waitlisted` booking.
- If no active booking exists:
  - Rolls back the transaction.
  - Finishes with `not_enrolled` or the previously completed cancellation result.
- Changes the booking status to `cancelled`.
- Sets the booking’s `cancelled_at` timestamp.

---

### 3. Handle a Waitlisted Cancellation

If the cancelled booking was `waitlisted`:

- No seat is released because waitlisted bookings do not consume seats.
- No other student is promoted.
- Commits the PostgreSQL transaction.
- Finishes with status `cancelled`.

---

### 4. Handle a Confirmed Cancellation

If the cancelled booking was `confirmed`:

- Selects and locks the oldest active waitlisted booking.
- Orders the waitlist by:
  - `created_at ASC`
  - `id ASC` as a tie-breaker

If a waitlisted booking exists:

- Changes the oldest waitlisted booking from `waitlisted` to `confirmed`.
- Keeps the confirmed seat count unchanged because one confirmed student left and another was promoted.
- Commits the PostgreSQL transaction.
- Finishes with status `cancelled_and_promoted`.

If no waitlisted booking exists:

- Decreases the class’s confirmed booking count by one.
- Commits the PostgreSQL transaction.
- Finishes with status `cancelled`.

---

### 5. Update Cache and Request Status

After the PostgreSQL transaction commits, the worker:

- Invalidates or refreshes the cached remaining-seat value in Redis.
- Updates the Redis cancellation request status with:
  - The final cancellation status.
  - The cancelled booking ID.
  - The promoted booking ID, if applicable.
  - The remaining seat count, if applicable.
  - The completion timestamp.
- Returns the cancellation result.

Redis updates happen after the PostgreSQL commit because PostgreSQL is the source of truth for bookings and seat availability.
