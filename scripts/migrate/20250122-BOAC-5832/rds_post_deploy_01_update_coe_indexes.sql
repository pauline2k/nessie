BEGIN TRANSACTION;

ALTER TABLE boac_advising_coe.students DROP COLUMN grad_term;
ALTER TABLE boac_advising_coe.students DROP COLUMN grad_year;

ALTER TABLE boac_advising_coe.students ADD COLUMN acad_status VARCHAR;
ALTER TABLE boac_advising_coe.students ADD COLUMN acad_status_term_id VARCHAR;
ALTER TABLE boac_advising_coe.students ADD COLUMN grad_term_id VARCHAR;

CREATE INDEX boac_advising_coe_acad_status ON boac_advising_coe.students (acad_status);

COMMIT;
