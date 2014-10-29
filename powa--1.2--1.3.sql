-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION powa" to load this file. \quit

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET escape_string_warning = off;
SET search_path = public, pg_catalog;

/* Add proctab related objects */

CREATE TYPE powa_cpu_history_record AS (
    ts timestamp with time zone,
    cpuuser bigint,
    cpunice bigint,
    cpusystem bigint,
    cpuidle bigint,
    cpuiowait bigint,
    cpuirq bigint,
    cpusoftirq bigint,
    cpusteal bigint
);

CREATE TABLE powa_cpu_history (
    coalesce_range tstzrange,
    records powa_cpu_history_record[]
);

CREATE TABLE powa_cpu_history_current (
    record powa_cpu_history_record
);

CREATE TYPE powa_mem_history_record AS (
    ts timestamp with time zone,
    memused bigint,
    memfree bigint,
    memshared bigint,
    membuffers bigint,
    memcached bigint,
    swapused bigint,
    swapfree bigint,
    swapcached bigint
);

CREATE TABLE powa_mem_history (
    coalesce_range tstzrange,
    records powa_mem_history_record[]
);

CREATE TABLE powa_mem_history_current (
    record powa_mem_history_record
);

CREATE TYPE powa_load_history_record AS (
    ts timestamp with time zone,
    load1 bigint,
    load5 bigint,
    load15 bigint
);

CREATE TABLE powa_load_history (
    coalesce_range tstzrange,
    records powa_load_history_record[]
);

CREATE TABLE powa_load_history_current (
    record powa_load_history_record
);

CREATE INDEX powa_cpu_history_ts ON powa_cpu_history USING gist (coalesce_range);
CREATE INDEX powa_mem_history_ts ON powa_mem_history USING gist (coalesce_range);
CREATE INDEX powa_load_history_ts ON powa_load_history USING gist (coalesce_range);

/* register proctab functions */
INSERT INTO powa_functions VALUES ('snapshot','powa_take_proctab_snapshot',false),('aggregate','powa_proctab_aggregate',true),('purge','powa_proctab_purge',true);

-- Mark all of proctab history tables as "to be dumped"
SELECT pg_catalog.pg_extension_config_dump('powa_cpu_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_cpu_history_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_mem_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_mem_history_current','');
SELECT pg_catalog.pg_extension_config_dump('powa_load_history','');
SELECT pg_catalog.pg_extension_config_dump('powa_load_history_current','');

/* pg_proctab functions, original work by Mark Wong */

CREATE OR REPLACE FUNCTION pg_cputime(
		OUT "user" BIGINT,
		OUT nice BIGINT,
		OUT system BIGINT,
		OUT idle BIGINT,
		OUT iowait BIGINT,
		OUT irq BIGINT,
		OUT softirq BIGINT,
		OUT steal BIGINT)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_cputime'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pg_loadavg(
		OUT load1 FLOAT,
		OUT load5 FLOAT,
		OUT load15 FLOAT,
		OUT last_pid INTEGER)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_loadavg'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION pg_memusage(
		OUT memused BIGINT,
		OUT memfree BIGINT,
		OUT memshared BIGINT,
		OUT membuffers BIGINT,
		OUT memcached BIGINT,
		OUT swapused BIGINT,
		OUT swapfree BIGINT,
		OUT swapcached BIGINT)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'pg_memusage'
LANGUAGE C IMMUTABLE STRICT;

/* proctab related functions */
CREATE OR REPLACE FUNCTION powa_take_proctab_snapshot() RETURNS void AS $PROC$
DECLARE
    result boolean;
BEGIN
    RAISE DEBUG 'running powa_take_proctab_snapshot';
        INSERT INTO powa_cpu_history_current (record)
        SELECT ROW(now(), cpu.user, cpu.nice, cpu.system, cpu.idle, cpu.iowait, cpu.irq,
                   cpu.softirq, cpu.steal)::powa_cpu_history_record AS record
          FROM pg_cputime() cpu;

        INSERT INTO powa_mem_history_current (record)
        SELECT ROW(now(), mem.memused, mem.memfree, mem.memshared, mem.membuffers, mem.memcached,
                   mem.swapused, mem.swapfree, mem.swapcached)::powa_mem_history_record AS record
          FROM pg_memusage() mem;

        INSERT INTO powa_load_history_current (record)
        SELECT ROW(now(), load.load1, load.load5, load.load15)::powa_load_history_record AS record
          FROM pg_loadavg() load;

        SELECT true::boolean INTO result; -- For now we don't care. What could we do on error except crash anyway?
END;
$PROC$ language plpgsql;

CREATE OR REPLACE FUNCTION powa_proctab_aggregate() RETURNS void AS $PROC$
BEGIN
    RAISE DEBUG 'running powa_proctab_aggregate';
    -- aggregate statements table
    LOCK TABLE powa_cpu_history_current IN SHARE MODE; -- prevent any other update
    LOCK TABLE powa_mem_history_current IN SHARE MODE; -- prevent any other update
    LOCK TABLE powa_load_history_current IN SHARE MODE; -- prevent any other update

    INSERT INTO powa_cpu_history
      SELECT tstzrange(min((record).ts), max((record).ts),'[]'),
             array_agg(record)
        FROM powa_cpu_history_current;
    TRUNCATE powa_cpu_history_current;

    INSERT INTO powa_mem_history
      SELECT tstzrange(min((record).ts), max((record).ts),'[]'),
             array_agg(record)
        FROM powa_mem_history_current;
    TRUNCATE powa_mem_history_current;

    INSERT INTO powa_load_history
      SELECT tstzrange(min((record).ts), max((record).ts),'[]'),
             array_agg(record)
        FROM powa_load_history_current;
    TRUNCATE powa_load_history_current;
 END;
$PROC$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION powa_proctab_purge() RETURNS void AS $PROC$
BEGIN
    RAISE DEBUG 'running powa_proctab_purge';
    -- Delete obsolete datas. We only bother with already coalesced data
    DELETE FROM powa_cpu_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);
    DELETE FROM powa_mem_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);
    DELETE FROM powa_load_history WHERE upper(coalesce_range)< (now() - current_setting('powa.retention')::interval);
END;
$PROC$ LANGUAGE plpgsql;

/* Data and sampling functions */

-- sample function for CPU
-- needs some more work to compute human readable units at output
CREATE OR REPLACE FUNCTION powa_proctab_get_cpu_statdata_sample (ts_start timestamp with time zone,
     ts_end timestamp with time zone, samples integer)
 RETURNS TABLE (ts timestamp with time zone, cpuuser float, cpunice float, cpusystem float, cpuidle float,
                cpuiowait float, cpuirq float, cpusoftirq float, cpusteal float)
AS $PROC$
BEGIN
   RETURN QUERY
   WITH cpu_history AS (
        SELECT (h.records).*
          FROM (SELECT unnest(records) AS records FROM powa_cpu_history) h
        UNION ALL
        SELECT (c.record).*
         FROM powa_cpu_history_current c
   ), cpu_history_number AS (
        SELECT row_number() OVER (order by cpu_history.ts) AS number, *
          FROM cpu_history
         WHERE cpu_history.ts >= ts_start
   ), sampled_cpu_history AS (
        SELECT *
          FROM cpu_history_number
         WHERE number % (int8larger((SELECT count(*) FROM cpu_history)/(samples+1),1) )=0
   ), cpu_history_differential AS (
        SELECT sh.ts,
               int8larger(lead(sh.cpuuser) OVER (querygroup) - sh.cpuuser,0) AS cpuuser,
               int8larger(lead(sh.cpunice) OVER (querygroup) - sh.cpunice,0) AS cpunice,
               int8larger(lead(sh.cpusystem) OVER (querygroup) - sh.cpusystem,0) AS cpusystem,
               int8larger(lead(sh.cpuidle) OVER (querygroup) - sh.cpuidle,0) AS cpuidle,
               int8larger(lead(sh.cpuiowait) OVER (querygroup) - sh.cpuiowait,0) AS cpuiowait,
               int8larger(lead(sh.cpuirq) OVER (querygroup) - sh.cpuirq,0) AS cpuirq,
               int8larger(lead(sh.cpusoftirq) OVER (querygroup) - sh.cpusoftirq,0) AS cpusoftirq,
               int8larger(lead(sh.cpusteal) OVER (querygroup) - sh.cpusteal,0) AS cpusteal
          FROM sampled_cpu_history sh
         WINDOW querygroup AS (ORDER BY sh.ts)
   ), cpu_history_percent AS (
        SELECT hs.ts, hs.cpuuser, hs.cpunice, hs.cpusystem, hs.cpuidle, hs.cpuiowait, hs.cpuirq, hs.cpusoftirq, hs.cpusteal,
               100.0::float / (hs.cpuuser + hs.cpunice + hs.cpusystem + hs.cpuidle + hs.cpuiowait + hs.cpuirq + hs.cpusoftirq + hs.cpusteal) AS scale
          FROM cpu_history_differential hs
         WHERE hs.cpuuser IS NOT NULL
   )
   SELECT pc.ts, pc.cpuuser*scale, pc.cpunice*scale, pc.cpusystem*scale, pc.cpuidle*scale,
          pc.cpuiowait*scale, pc.cpuirq*scale, pc.cpusoftirq*scale, pc.cpusteal*scale
     FROM cpu_history_percent pc
END;
$PROC$ LANGUAGE plpgsql;

-- sample function for Memory
CREATE OR REPLACE FUNCTION powa_proctab_get_mem_statdata_sample (ts_start timestamp with time zone,
     ts_end timestamp with time zone, samples integer)
 RETURNS TABLE (ts timestamp with time zone, memused bigint, memfree bigint, memshared bigint,
                membuffers bigint, memcached bigint, swapused bigint, swapfree bigint, swapcached bigint)
AS $PROC$
BEGIN
   RETURN QUERY
   WITH mem_history AS (
        SELECT (h.records).*
          FROM (SELECT unnest(records) AS records FROM powa_mem_history) h
        UNION ALL
        SELECT (c.record).*
         FROM powa_mem_history_current c
   ), mem_history_number AS (
        SELECT row_number() OVER (order by mem_history.ts) AS number, *
          FROM mem_history
         WHERE mem_history.ts >= ts_start
   ), sampled_mem_history AS (
        SELECT *
          FROM mem_history_number
         WHERE number % (int8larger((SELECT count(*) FROM mem_history)/(samples+1),1) )=0
   )
   SELECT hd.ts, hd.memused, hd.memfree, hd.memshared, hd.membuffers, hd.memcached,
          hd.swapused, hd.swapfree, hd.swapcached
     FROM sampled_mem_history hd
    WHERE hd.memused IS NOT NULL;
END;
$PROC$ LANGUAGE plpgsql;

-- sample function for Load Average
CREATE OR REPLACE FUNCTION powa_proctab_get_load_statdata_sample (ts_start timestamp with time zone,
     ts_end timestamp with time zone, samples integer)
 RETURNS TABLE (ts timestamp with time zone, load1 bigint, load5 bigint, load15 bigint)
AS $PROC$
BEGIN
   RETURN QUERY
   WITH load_history AS (
        SELECT (h.records).*
          FROM (SELECT unnest(records) AS records FROM powa_load_history) h
        UNION ALL
        SELECT (c.record).*
         FROM powa_load_history_current c
   ), load_history_number AS (
        SELECT row_number() OVER (order by load_history.ts) AS number, *
          FROM load_history
         WHERE load_history.ts >= ts_start
   ), sampled_load_history AS (
        SELECT *
          FROM load_history_number
         WHERE number % (int8larger((SELECT count(*) FROM load_history)/(samples+1),1) )=0
   )
   SELECT hd.ts, hd.load1, hd.load5, hd.load15
     FROM sampled_load_history hd
    WHERE hd.load1 IS NOT NULL;
END;
$PROC$ LANGUAGE plpgsql;
