
/*
 * Copyright (C) 2008 Mark Wong
 * Additionnal work by Thomas Reiss, 2014
 */

#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "access/htup_details.h"
#include "utils/tuplestore.h"
#include "storage/fd.h"
#include "utils/builtins.h"

#include <sys/vfs.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/param.h>
#include <executor/spi.h>

#ifdef __linux
#include <ctype.h>
#include <linux/magic.h>
#endif

#define BIGINT_LEN 20

#ifdef __linux__
#define PROCFS "/proc"

#define SKIP_TOKEN(p) \
		/* Skipping leading white space. */ \
		while (isspace(*p)) \
			p++; \
		/* Skip token. */ \
		while (*p && !isspace(*p)) \
			p++; \
		/* Skipping trailing white space. */ \
		while (isspace(*p)) \
			p++;
#endif                          /* __linux__ */

Datum       pg_cputime(PG_FUNCTION_ARGS);
Datum       pg_loadavg(PG_FUNCTION_ARGS);
Datum       pg_memusage(PG_FUNCTION_ARGS);
Datum       pg_diskusage(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_cputime);
PG_FUNCTION_INFO_V1(pg_loadavg);
PG_FUNCTION_INFO_V1(pg_memusage);
PG_FUNCTION_INFO_V1(pg_diskusage);

Datum pg_cputime(PG_FUNCTION_ARGS)
{
    struct statfs sb;
    int         fd;
    int         len;
    char        buffer[4096];
    int64       cpuuser = 0;
    int64       cpunice = 0;
    int64       cpusys = 0;
    int64       cpuidle = 0;
    int64       cpuiowait = 0;
    int64       cpuirq = 0;
    int64       cpusoftirq = 0;
    int64       cpusteal = 0;
    TupleDesc   tupleDesc;
    HeapTuple   tuple;
    Datum       values[8];
    bool        nulls[8];
    Datum       result;

    if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");
    Assert(tupleDesc->natts == lengthof(values));

#ifdef __linux__
    /*
       Check if /proc is mounted. 
     */
    if (statfs(PROCFS, &sb) < 0 || sb.f_type != PROC_SUPER_MAGIC)
      {
          elog(ERROR, "proc filesystem not mounted on " PROCFS "\n");
          return 0;
      }

    snprintf(buffer, sizeof(buffer) - 1, "%s/stat", PROCFS);
    fd = open(buffer, O_RDONLY);
    if (fd == -1)
      {
          elog(ERROR, "could not open file '%s'", buffer);
          return 0;
      }
    if ((len = read(fd, buffer, sizeof(buffer) - 1)) < 0)
      {
          elog(ERROR, "could not read file '/proc/stat'");
      }

    close(fd);
    buffer[len] = '\0';

    sscanf(buffer, "cpu %lu %lu %lu %lu %lu %lu %lu %lu",
           &cpuuser, &cpunice, &cpusys, &cpuidle, &cpuiowait, &cpuirq,
           &cpusoftirq, &cpusteal);
#endif                          /* __linux__ */

    elog(DEBUG5, "pg_cputime: user = %ld", cpuuser);
    elog(DEBUG5, "pg_cputime: nice = %ld", cpunice);
    elog(DEBUG5, "pg_cputime: system = %ld", cpusys);
    elog(DEBUG5, "pg_cputime: idle = %ld", cpuidle);
    elog(DEBUG5, "pg_cputime: iowait = %ld", cpuiowait);
    elog(DEBUG5, "pg_cputime: irq = %ld", cpuirq);
    elog(DEBUG5, "pg_cputime: softirq = %ld", cpusoftirq);
    elog(DEBUG5, "pg_cputime: steal = %ld", cpusteal);

    memset(nulls, 0, sizeof(nulls));
    memset(values, 0, sizeof(values));
    values[0] = Int64GetDatum(cpuuser);
    values[1] = Int64GetDatum(cpunice);
    values[2] = Int64GetDatum(cpusys);
    values[3] = Int64GetDatum(cpuidle);
    values[4] = Int64GetDatum(cpuiowait);
    values[5] = Int64GetDatum(cpuirq);
    values[6] = Int64GetDatum(cpusoftirq);
    values[7] = Int64GetDatum(cpusteal);

    tuple = heap_form_tuple(tupleDesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

Datum pg_loadavg(PG_FUNCTION_ARGS)
{
    struct statfs sb;
    int         fd;
    int         len;
    char        buffer[4096];
    float       loadavg1 = 0.0;
    float       loadavg5 = 0.0;
    float       loadavg15 = 0.0;
    TupleDesc   tupleDesc;
    HeapTuple   tuple;
    Datum       values[3];
    bool        nulls[3];
    Datum       result;

    if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");
    Assert(tupleDesc->natts == lengthof(values));

#ifdef __linux__
    /*
       Check if /proc is mounted. 
     */
    if (statfs(PROCFS, &sb) < 0 || sb.f_type != PROC_SUPER_MAGIC)
      {
          elog(ERROR, "proc filesystem not mounted on " PROCFS "\n");
          return 0;
      }

    snprintf(buffer, sizeof(buffer) - 1, "%s/loadavg", PROCFS);
    fd = open(buffer, O_RDONLY);
    if (fd == -1)
      {
          elog(ERROR, "'%s' not found", buffer);
          return 0;
      }
    len = read(fd, buffer, sizeof(buffer) - 1);
    close(fd);
    buffer[len] = '\0';
    elog(DEBUG5, "pg_loadavg: %s", buffer);

    sscanf(buffer, "%f %f %f", &loadavg1, &loadavg5, &loadavg15);

    memset(nulls, 0, sizeof(nulls));
    memset(values, 0, sizeof(values));
    values[0] = Float4GetDatum(loadavg1);
    values[1] = Float4GetDatum(loadavg5);
    values[2] = Float4GetDatum(loadavg15);

#endif                          /* __linux__ */

    elog(DEBUG5, "pg_loadavg: load1 = %f", loadavg1);
    elog(DEBUG5, "pg_loadavg: load5 = %f", loadavg5);
    elog(DEBUG5, "pg_loadavg: load15 = %f", loadavg15);

    tuple = heap_form_tuple(tupleDesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

Datum pg_memusage(PG_FUNCTION_ARGS)
{
    int64       memused = 0;
    int64       memfree = 0;
    int64       memshared = 0;
    int64       membuffers = 0;
    int64       memcached = 0;
    int64       swapused = 0;
    int64       swapfree = 0;
    int64       swapcached = 0;

    int64       memtotal = 0;
    int64       swaptotal = 0;

    TupleDesc   tupleDesc;
    HeapTuple   tuple;
    Datum       values[8];
    bool        nulls[8];
    Datum       result;

#ifdef __linux__
    struct statfs sb;
    int         fd;
    int         len;
    char        buffer[4096];
    char       *p;
#endif                          /* __linux__ */

    if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");
    Assert(tupleDesc->natts == lengthof(values));

#ifdef __linux__
    /*
       Check if /proc is mounted. 
     */
    if (statfs(PROCFS, &sb) < 0 || sb.f_type != PROC_SUPER_MAGIC)
      {
          elog(ERROR, "proc filesystem not mounted on " PROCFS "\n");
          return 0;
      }

    snprintf(buffer, sizeof(buffer) - 1, "%s/meminfo", PROCFS);
    fd = open(buffer, O_RDONLY);
    if (fd == -1)
      {
          elog(ERROR, "'%s' not found", buffer);
          return 0;
      }
    len = read(fd, buffer, sizeof(buffer) - 1);
    close(fd);
    buffer[len] = '\0';
    elog(DEBUG5, "pg_memusage: %s", buffer);

    p = buffer - 1;

    while (p != NULL)
      {
          ++p;
          if (strncmp(p, "Buffers:", 8) == 0)
            {
                SKIP_TOKEN(p);
                membuffers = strtoul(p, &p, 10);
            }
          else if (strncmp(p, "Cached:", 7) == 0)
            {
                SKIP_TOKEN(p);
                memcached = strtoul(p, &p, 10);
            }
          else if (strncmp(p, "MemFree:", 8) == 0)
            {
                SKIP_TOKEN(p);
                memfree = strtoul(p, &p, 10);
                memused = memtotal - memfree;
            }
          else if (strncmp(p, "MemShared:", 10) == 0)
            {
                /*
                   does not exist anymore in kernel 3.13
                   * TODO: remove this counter
                 */
                SKIP_TOKEN(p);
                memshared = strtoul(p, &p, 10);
            }
          else if (strncmp(p, "Shmem:", 6) == 0)
            {
                SKIP_TOKEN(p);
                memshared = strtoul(p, &p, 10);
            }
          else if (strncmp(p, "MemTotal:", 9) == 0)
            {
                SKIP_TOKEN(p);
                memtotal = strtoul(p, &p, 10);
            }
          else if (strncmp(p, "SwapFree:", 9) == 0)
            {
                SKIP_TOKEN(p);
                swapfree = strtoul(p, &p, 10);
                swapused = swaptotal - swapfree;
            }
          else if (strncmp(p, "SwapCached:", 11) == 0)
            {
                SKIP_TOKEN(p);
                swapcached = strtoul(p, &p, 10);
            }
          else if (strncmp(p, "SwapTotal:", 10) == 0)
            {
                SKIP_TOKEN(p);
                swaptotal = strtoul(p, &p, 10);
            }
          p = strchr(p, '\n');
      }
#endif                          /* __linux__ */

    memset(nulls, 0, sizeof(nulls));
    memset(values, 0, sizeof(values));
    values[0] = Int64GetDatum(memused);
    values[1] = Int64GetDatum(memfree);
    values[2] = Int64GetDatum(memshared);
    values[3] = Int64GetDatum(membuffers);
    values[4] = Int64GetDatum(memcached);
    values[5] = Int64GetDatum(swapused);
    values[6] = Int64GetDatum(swapfree);
    values[7] = Int64GetDatum(swapcached);

    tuple = heap_form_tuple(tupleDesc, values, nulls);
    result = HeapTupleGetDatum(tuple);

    PG_RETURN_DATUM(result);
}

/* SRF function that returns diskstats */
Datum pg_diskusage(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    MemoryContext per_query_ctx;
    MemoryContext oldcontext;
    TupleDesc   tupleDesc;
    Tuplestorestate *tupleStore;

    Datum       values[14];
    bool        nulls[14];

    char        buffer[4096];
    struct statfs sb;
    FILE       *fd;
    int         ret;

    int         major = 0;
    int         minor = 0;
    char       *device_name = NULL;
    int64       reads_completed = 0;
    int64       reads_merged = 0;
    int64       sectors_read = 0;
    int64       readtime = 0;
    int64       writes_completed = 0;
    int64       writes_merged = 0;
    int64       sectors_written = 0;
    int64       writetime = 0;
    int64       current_io = 0;
    int64       iotime = 0;
    int64       totaliotime = 0;

    elog(DEBUG5, "pg_diskusage: Entering stored function.");

    if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg
                 ("set-valued function called in context that cannot accept a set")));
    if (!(rsinfo->allowedModes & SFRM_Materialize))
        ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("materialize mode required, but it is not "
                        "allowed in this context")));

    per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
    oldcontext = MemoryContextSwitchTo(per_query_ctx);

    /*
       Build a tuple descriptor for our result type 
     */
    if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
        elog(ERROR, "return type must be a row type");

    tupleStore = tuplestore_begin_heap(true, false, work_mem);
    rsinfo->returnMode = SFRM_Materialize;
    rsinfo->setResult = tupleStore;
    rsinfo->setDesc = tupleDesc;

    MemoryContextSwitchTo(oldcontext);

    memset(nulls, 0, sizeof(nulls));
    memset(values, 0, sizeof(values));

#ifdef __linux__

    if (statfs("/proc", &sb) < 0 || sb.f_type != PROC_SUPER_MAGIC)
      {
          elog(ERROR, "proc filesystem not mounted on /proc\n");
          return (Datum) 0;
      }

    if ((fd = AllocateFile("/proc/diskstats", PG_BINARY_R)) == NULL)
      {
          elog(ERROR, "'%s' not found", buffer);
          return (Datum) 0;
      }

    device_name = buffer;
    while ((ret =
            fscanf(fd, "%d %d %s %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu %lu",
                   &major, &minor, device_name, &reads_completed,
                   &reads_merged, &sectors_read, &readtime, &writes_completed,
                   &writes_merged, &sectors_written, &writetime, &current_io,
                   &iotime, &totaliotime)) != EOF)
      {
          values[0] = Int32GetDatum(major);
          values[1] = Int32GetDatum(minor);
          values[2] = CStringGetTextDatum(device_name);
          values[3] = Int64GetDatumFast(reads_completed);
          values[4] = Int64GetDatumFast(reads_merged);
          values[5] = Int64GetDatumFast(sectors_read);
          values[6] = Int64GetDatumFast(readtime);
          values[7] = Int64GetDatumFast(writes_completed);
          values[8] = Int64GetDatumFast(writes_merged);
          values[9] = Int64GetDatumFast(sectors_written);
          values[10] = Int64GetDatumFast(writetime);
          values[11] = Int64GetDatumFast(current_io);
          values[12] = Int64GetDatumFast(iotime);
          values[13] = Int64GetDatumFast(totaliotime);
          tuplestore_putvalues(tupleStore, tupleDesc, values, nulls);
      }
    FreeFile(fd);
#endif                          /* __linux */

    tuplestore_donestoring(tupleStore);

    return (Datum) 0;
}
