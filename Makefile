MODULE_big = powa
OBJS = powa.o pg_proctab.o
EXTENSION = powa
DATA = powa--1.1.sql powa--1.2.sql powa--1.1--1.2.sql powa--1.3.sql
DOCS = README.md

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
