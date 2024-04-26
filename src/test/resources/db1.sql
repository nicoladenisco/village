CREATE TABLE stp.transcode
(
    app VARCHAR(16) NOT NULL,
    tipo VARCHAR(16) NOT NULL,
    codice_vero VARCHAR(64) NOT NULL,
    codice_app VARCHAR(64) NOT NULL,
    CONSTRAINT transcode_pkey PRIMARY KEY (app, tipo, codice_vero)
);

CREATE INDEX idx_transcode_1
    ON stp.transcode (app, codice_app);

INSERT INTO stp.transcode (app, tipo, codice_vero, codice_app)
  VALUES ('a', 'a', 'CODVERO1', 'CODAPP1');
INSERT INTO stp.transcode (app, tipo, codice_vero, codice_app)
  VALUES ('a', 'a', 'CODVERO2', 'CODAPP2');
INSERT INTO stp.transcode (app, tipo, codice_vero, codice_app)
  VALUES ('a', 'a', 'CODVERO3', 'CODAPP3');

INSERT INTO stp.transcode (app, tipo, codice_vero, codice_app)
  VALUES ('a', 'b', 'CODVERO4', 'CODAPP4');
INSERT INTO stp.transcode (app, tipo, codice_vero, codice_app)
  VALUES ('a', 'b', 'CODVERO5', 'CODAPP5');
INSERT INTO stp.transcode (app, tipo, codice_vero, codice_app)
  VALUES ('a', 'b', 'CODVERO6', 'CODAPP6');

INSERT INTO stp.transcode (app, tipo, codice_vero, codice_app)
  VALUES ('c', 'c', 'CODVERO7', 'CODAPP7');
INSERT INTO stp.transcode (app, tipo, codice_vero, codice_app)
  VALUES ('c', 'c', 'CODVERO8', 'CODAPP8');
INSERT INTO stp.transcode (app, tipo, codice_vero, codice_app)
  VALUES ('c', 'c', 'CODVERO9', 'CODAPP9');


