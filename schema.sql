BEGIN;

CREATE TABLE schools (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    created_at  BIGINT NOT NULL DEFAULT (FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::BIGINT ),
    deleted_at  BIGINT,
);

CREATE TABLE users (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    school_id   BIGINT NOT NULL,
    email       VARCHAR(320) NOT NULL,
    full_name   VARCHAR(255) NOT NULL,
    role        VARCHAR(30) NOT NULL,
    created_at  BIGINT NOT NULL DEFAULT (FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::BIGINT ),
    deleted_at  BIGINT,

    CONSTRAINT fk_users_school
        FOREIGN KEY (school_id)
        REFERENCES schools (id),

    CONSTRAINT chk_users_role
        CHECK (
            role IN ('student', 'instructor', 'admin')
        )
);

CREATE TABLE courses (
    id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    school_id    BIGINT NOT NULL,
    title        VARCHAR(255) NOT NULL,
    description  TEXT,
    status       VARCHAR(20) NOT NULL DEFAULT 'draft',
    created_at   BIGINT NOT NULL DEFAULT (FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::BIGINT ),
    deleted_at   BIGINT,

    CONSTRAINT fk_courses_school
        FOREIGN KEY (school_id)
        REFERENCES schools (id),

    CONSTRAINT chk_courses_status
        CHECK (
            status IN ('draft', 'published', 'archived')
        )
);

CREATE TABLE live_classes (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    school_id         BIGINT NOT NULL,
    course_id         BIGINT NOT NULL,
    instructor_id     BIGINT,
    title             VARCHAR(255) NOT NULL,
    starts_at         BIGINT NOT NULL,
    duration_minutes  INTEGER NOT NULL,
    seat_capacity     INTEGER NOT NULL,
    price             FLOAT NOT NULL DEFAULT 0,
    status            VARCHAR(20) NOT NULL DEFAULT 'scheduled',
    meeting_url       TEXT,
    created_at        BIGINT NOT NULL DEFAULT (FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::BIGINT ),
    deleted_at        BIGINT,

    CONSTRAINT fk_live_classes_school
        FOREIGN KEY (school_id)
        REFERENCES schools (id),

    CONSTRAINT fk_live_classes_course
        FOREIGN KEY (school_id, course_id)
        REFERENCES courses (school_id, id),

    CONSTRAINT fk_live_classes_instructor
        FOREIGN KEY (school_id, instructor_id)
        REFERENCES users (school_id, id),

    CONSTRAINT chk_live_classes_duration
        CHECK (duration_minutes > 0),

    CONSTRAINT chk_live_classes_capacity
        CHECK (seat_capacity > 0),

    CONSTRAINT chk_live_classes_status
        CHECK (
            status IN (
                'scheduled',
                'live',
                'completed',
                'cancelled'
            )
        )
);

CREATE TABLE class_bookings (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    school_id       BIGINT NOT NULL,
    live_class_id   BIGINT NOT NULL,
    student_id      BIGINT NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'confirmed',
    created_at      BIGINT NOT NULL DEFAULT (FLOOR(EXTRACT(EPOCH FROM clock_timestamp()))::BIGINT ),
    cancelled_at    BIGINT,
    deleted_at      BIGINT,

    CONSTRAINT fk_class_bookings_school
        FOREIGN KEY (school_id)
        REFERENCES schools (id),

    CONSTRAINT fk_class_bookings_live_class
        FOREIGN KEY (school_id, live_class_id)
        REFERENCES live_classes (school_id, id),

    CONSTRAINT fk_class_bookings_student
        FOREIGN KEY (school_id, student_id)
        REFERENCES users (school_id, id),

    CONSTRAINT chk_class_bookings_status
        CHECK (
            status IN (
                'confirmed',
                'waitlisted',
                'cancelled',
                'attended',
                'no_show'
            )
        )
);

CREATE UNIQUE INDEX uq_users_active_email
    ON users (
        school_id,
        LOWER(email)
    )
    WHERE deleted_at IS NULL;

CREATE UNIQUE INDEX uq_class_bookings_active_student
    ON class_bookings (
        school_id,
        live_class_id,
        student_id
    )
    WHERE deleted_at IS NULL
    AND status IN ('confirmed', 'waitlisted');

CREATE INDEX idx_class_bookings_waitlist
    ON class_bookings (
        school_id,
        live_class_id,
        booked_at,
        id
    )
    WHERE deleted_at IS NULL
        AND status = 'waitlisted';

CREATE INDEX idx_users_active_school
    ON users (school_id)
    WHERE deleted_at IS NULL;

CREATE INDEX idx_courses_active_school
    ON courses (school_id)
    WHERE deleted_at IS NULL;

CREATE INDEX idx_live_classes_active_course_start
    ON live_classes (
        school_id,
        course_id,
        starts_at
    )
    WHERE deleted_at IS NULL;

CREATE INDEX idx_live_classes_active_instructor_start
    ON live_classes (
        school_id,
        instructor_id,
        starts_at
    )
    WHERE deleted_at IS NULL;

CREATE INDEX idx_class_bookings_active_class_status
    ON class_bookings (
        school_id,
        live_class_id,
        status
    )
    WHERE deleted_at IS NULL;

CREATE INDEX idx_class_bookings_active_student
    ON class_bookings (
        school_id,
        student_id
    )
    WHERE deleted_at IS NULL;

COMMIT;
