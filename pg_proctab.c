
/*
 * Copyright (C) 2008 Mark Wong
 * Additionnal work by Thomas Reiss, 2014
 */

#include "postgres.h"
#include <string.h>
#include "fmgr.h"
#include "funcapi.h"
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

#define FULLCOMM_LEN 1024
#define BIGINT_LEN 20
#define FLOAT_LEN 20
#define INTEGER_LEN 10

#ifdef PG91
#define GET_PIDS \
		"SELECT procpid " \
		"FROM pg_stat_activity"
#else
#define GET_PIDS \
		"SELECT pid " \
		"FROM pg_stat_activity"
#endif                          /* PG91 */

#ifdef __linux__
#define GET_VALUE(value) \
		p = strchr(p, ':'); \
		++p; \
		++p; \
		q = strchr(p, '\n'); \
		len = q - p; \
		if (len >= BIGINT_LEN) \
		{ \
			elog(ERROR, "value is larger than the buffer: %d\n", __LINE__); \
			return 0; \
		} \
		strncpy(value, p, len); \
		value[len] = '\0';

#define PROCFS "/proc"

#define GET_NEXT_VALUE(p, q, value, length, msg, delim) \
        if ((q = strchr(p, delim)) == NULL) \
        { \
            elog(ERROR, msg); \
            return 0; \
        } \
        length = q - p; \
        strncpy(value, p, length); \
        value[length] = '\0'; \
        p = q + 1;

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

enum cputime { i_user, i_nice_c, i_system, i_idle, i_iowait, i_irq, i_softirq,
    i_steal
};
enum loadavg { i_load1, i_load5, i_load15, i_last_pid };
enum memusage { i_memused, i_memfree, i_memshared, i_membuffers, i_memcached,
    i_swapused, i_swapfree, i_swapcached
};

int         get_cputime(char **);
int         get_loadavg(char **);
int         get_memusage(char **);

Datum       pg_cputime(PG_FUNCTION_ARGS);
Datum       pg_loadavg(PG_FUNCTION_ARGS);
Datum       pg_memusage(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_cputime);
PG_FUNCTION_INFO_V1(pg_loadavg);
PG_FUNCTION_INFO_V1(pg_memusage);

Datum pg_cputime(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    int         call_cntr;
    int         max_calls;
    TupleDesc   tupdesc;
    AttInMetadata *attinmeta;

    elog(DEBUG5, "pg_cputime: Entering stored function.");

    /*
       stuff done only on the first call of the function 
     */
    if (SRF_IS_FIRSTCALL())
      {
          MemoryContext oldcontext;

          /*
             create a function context for cross-call persistence 
           */
          funcctx = SRF_FIRSTCALL_INIT();

          /*
             switch to memory context appropriate for multiple function calls 
           */
          oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

          /*
             Build a tuple descriptor for our result type 
           */
          if (get_call_result_type(fcinfo, NULL, &tupdesc) !=
              TYPEFUNC_COMPOSITE)
              ereport(ERROR,
                      (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("function returning record called in context "
                              "that cannot accept type record")));

          /*
           * generate attribute metadata needed later to produce tuples from raw
           * C strings
           */
          attinmeta = TupleDescGetAttInMetadata(tupdesc);
          funcctx->attinmeta = attinmeta;

          funcctx->max_calls = 1;

          MemoryContextSwitchTo(oldcontext);
      }

    /*
       stuff done on every call of the function 
     */
    funcctx = SRF_PERCALL_SETUP();

    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;

    if (call_cntr < max_calls)  /* do when there is more left to send */
      {
          HeapTuple   tuple;
          Datum       result;

          char      **values = NULL;

          values = (char **) palloc(8 * sizeof(char *));
          values[i_user] = (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_nice_c] = (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_system] = (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_idle] = (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_iowait] = (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_irq] = (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_softirq] = (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_steal] = (char *) palloc((BIGINT_LEN + 1) * sizeof(char));

          if (get_cputime(values) == 0)
              SRF_RETURN_DONE(funcctx);

          /*
             build a tuple 
           */
          tuple = BuildTupleFromCStrings(attinmeta, values);

          /*
             make the tuple into a datum 
           */
          result = HeapTupleGetDatum(tuple);

          SRF_RETURN_NEXT(funcctx, result);
      }
    else                        /* do when there is no more left */
      {
          SRF_RETURN_DONE(funcctx);
      }
}

int get_cputime(char **values)
{
#ifdef __linux__
    struct statfs sb;
    int         fd;
    int         len;
    char        buffer[4096];
    char       *p;
    char       *q;

    int         length;

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
          elog(ERROR, "'%s' not found", buffer);
          return 0;
      }
    len = read(fd, buffer, sizeof(buffer) - 1);
    close(fd);
    buffer[len] = '\0';
    elog(DEBUG5, "pg_cputime: %s", buffer);

    p = buffer;

    SKIP_TOKEN(p);              /* skip cpu */

    /*
       user 
     */
    GET_NEXT_VALUE(p, q, values[i_user], length, "user not found", ' ');

    /*
       nice 
     */
    GET_NEXT_VALUE(p, q, values[i_nice_c], length, "nice not found", ' ');

    /*
       system 
     */
    GET_NEXT_VALUE(p, q, values[i_system], length, "system not found", ' ');

    /*
       idle 
     */
    GET_NEXT_VALUE(p, q, values[i_idle], length, "idle not found", ' ');

    /*
       iowait 
     */
    GET_NEXT_VALUE(p, q, values[i_iowait], length, "iowait not found", ' ');

    /*
       irq 
     */
    GET_NEXT_VALUE(p, q, values[i_irq], length, "irq not found", ' ');

    /*
       softirq 
     */
    GET_NEXT_VALUE(p, q, values[i_softirq], length, "softirq not found", ' ');

    /*
       steal 
     */
    GET_NEXT_VALUE(p, q, values[i_steal], length, "steal not found", ' ');
#endif                          /* __linux__ */

    elog(DEBUG5, "pg_cputime: [%d] user = %s", (int) i_user, values[i_user]);
    elog(DEBUG5, "pg_cputime: [%d] nice = %s", (int) i_nice_c,
         values[i_nice_c]);
    elog(DEBUG5, "pg_cputime: [%d] system = %s", (int) i_system,
         values[i_system]);
    elog(DEBUG5, "pg_cputime: [%d] idle = %s", (int) i_idle, values[i_idle]);
    elog(DEBUG5, "pg_cputime: [%d] iowait = %s", (int) i_iowait,
         values[i_iowait]);
    elog(DEBUG5, "pg_cputime: [%d] irq = %s", (int) i_irq, values[i_irq]);
    elog(DEBUG5, "pg_cputime: [%d] softirq = %s", (int) i_softirq,
         values[i_softirq]);
    elog(DEBUG5, "pg_cputime: [%d] steal = %s", (int) i_steal,
         values[i_steal]);

    return 1;
}

Datum pg_loadavg(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    int         call_cntr;
    int         max_calls;
    TupleDesc   tupdesc;
    AttInMetadata *attinmeta;

    elog(DEBUG5, "pg_loadavg: Entering stored function.");

    /*
       stuff done only on the first call of the function 
     */
    if (SRF_IS_FIRSTCALL())
      {
          MemoryContext oldcontext;

          /*
             create a function context for cross-call persistence 
           */
          funcctx = SRF_FIRSTCALL_INIT();

          /*
             switch to memory context appropriate for multiple function calls 
           */
          oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

          /*
             Build a tuple descriptor for our result type 
           */
          if (get_call_result_type(fcinfo, NULL, &tupdesc) !=
              TYPEFUNC_COMPOSITE)
              ereport(ERROR,
                      (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("function returning record called in context "
                              "that cannot accept type record")));

          /*
           * generate attribute metadata needed later to produce tuples from raw
           * C strings
           */
          attinmeta = TupleDescGetAttInMetadata(tupdesc);
          funcctx->attinmeta = attinmeta;

          funcctx->max_calls = 1;

          MemoryContextSwitchTo(oldcontext);
      }

    /*
       stuff done on every call of the function 
     */
    funcctx = SRF_PERCALL_SETUP();

    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;

    if (call_cntr < max_calls)  /* do when there is more left to send */
      {
          HeapTuple   tuple;
          Datum       result;

          char      **values = NULL;

          values = (char **) palloc(4 * sizeof(char *));
          values[i_load1] = (char *) palloc((FLOAT_LEN + 1) * sizeof(char));
          values[i_load5] = (char *) palloc((FLOAT_LEN + 1) * sizeof(char));
          values[i_load15] = (char *) palloc((FLOAT_LEN + 1) * sizeof(char));
          values[i_last_pid] =
              (char *) palloc((INTEGER_LEN + 1) * sizeof(char));

          if (get_loadavg(values) == 0)
              SRF_RETURN_DONE(funcctx);

          /*
             build a tuple 
           */
          tuple = BuildTupleFromCStrings(attinmeta, values);

          /*
             make the tuple into a datum 
           */
          result = HeapTupleGetDatum(tuple);

          SRF_RETURN_NEXT(funcctx, result);
      }
    else                        /* do when there is no more left */
      {
          SRF_RETURN_DONE(funcctx);
      }
}

int get_loadavg(char **values)
{
#ifdef __linux__
    int         length;

    struct statfs sb;
    int         fd;
    int         len;
    char        buffer[4096];
    char       *p;
    char       *q;

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

    p = buffer;

    /*
       load1 
     */
    GET_NEXT_VALUE(p, q, values[i_load1], length, "load1 not found", ' ');

    /*
       load5 
     */
    GET_NEXT_VALUE(p, q, values[i_load5], length, "load5 not found", ' ');

    /*
       load15 
     */
    GET_NEXT_VALUE(p, q, values[i_load15], length, "load15 not found", ' ');

    SKIP_TOKEN(p);              /* skip running/tasks */

    /*
       last_pid 
     */
    /*
     * It appears sometimes this is the last item in /proc/PID/stat and
     * sometimes it's not, depending on the version of the kernel and
     * possibly the architecture.  So first test if it is the last item
     * before determining how to deliminate it.
     */
    if (strchr(p, ' ') == NULL)
      {
          GET_NEXT_VALUE(p, q, values[i_last_pid], length,
                         "last_pid not found", '\n');
      }
    else
      {
          GET_NEXT_VALUE(p, q, values[i_last_pid], length,
                         "last_pid not found", ' ');
      }
#endif                          /* __linux__ */

    elog(DEBUG5, "pg_loadavg: [%d] load1 = %s", (int) i_load1,
         values[i_load1]);
    elog(DEBUG5, "pg_loadavg: [%d] load5 = %s", (int) i_load5,
         values[i_load5]);
    elog(DEBUG5, "pg_loadavg: [%d] load15 = %s", (int) i_load15,
         values[i_load15]);
    elog(DEBUG5, "pg_loadavg: [%d] last_pid = %s", (int) i_last_pid,
         values[i_last_pid]);

    return 1;
}

Datum pg_memusage(PG_FUNCTION_ARGS)
{
    FuncCallContext *funcctx;
    int         call_cntr;
    int         max_calls;
    TupleDesc   tupdesc;
    AttInMetadata *attinmeta;


    elog(DEBUG5, "pg_memusage: Entering stored function.");

    /*
       stuff done only on the first call of the function 
     */
    if (SRF_IS_FIRSTCALL())
      {
          MemoryContext oldcontext;

          /*
             create a function context for cross-call persistence 
           */
          funcctx = SRF_FIRSTCALL_INIT();

          /*
             switch to memory context appropriate for multiple function calls 
           */
          oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

          /*
             Build a tuple descriptor for our result type 
           */
          if (get_call_result_type(fcinfo, NULL, &tupdesc) !=
              TYPEFUNC_COMPOSITE)
              ereport(ERROR,
                      (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("function returning record called in context "
                              "that cannot accept type record")));

          /*
           * generate attribute metadata needed later to produce tuples from raw
           * C strings
           */
          attinmeta = TupleDescGetAttInMetadata(tupdesc);
          funcctx->attinmeta = attinmeta;

          funcctx->max_calls = 1;

          MemoryContextSwitchTo(oldcontext);
      }

    /*
       stuff done on every call of the function 
     */
    funcctx = SRF_PERCALL_SETUP();

    call_cntr = funcctx->call_cntr;
    max_calls = funcctx->max_calls;
    attinmeta = funcctx->attinmeta;

    if (call_cntr < max_calls)  /* do when there is more left to send */
      {
          HeapTuple   tuple;
          Datum       result;

          char      **values = NULL;

          values = (char **) palloc(8 * sizeof(char *));
          values[i_memused] = (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_memfree] = (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_memshared] =
              (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_membuffers] =
              (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_memcached] =
              (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_swapused] =
              (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_swapfree] =
              (char *) palloc((BIGINT_LEN + 1) * sizeof(char));
          values[i_swapcached] =
              (char *) palloc((BIGINT_LEN + 1) * sizeof(char));

          if (get_memusage(values) == 0)
              SRF_RETURN_DONE(funcctx);

          /*
             build a tuple 
           */
          tuple = BuildTupleFromCStrings(attinmeta, values);

          /*
             make the tuple into a datum 
           */
          result = HeapTupleGetDatum(tuple);

          SRF_RETURN_NEXT(funcctx, result);
      }
    else                        /* do when there is no more left */
      {
          SRF_RETURN_DONE(funcctx);
      }
}

int get_memusage(char **values)
{
#ifdef __linux__
    int         length;
    unsigned long memfree = 0;
    unsigned long memtotal = 0;
    unsigned long swapfree = 0;
    unsigned long swaptotal = 0;

    struct statfs sb;
    int         fd;
    int         len;
    char        buffer[4096];
    char       *p;
    char       *q;

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

    values[i_memshared][0] = '0';
    values[i_memshared][1] = '\0';

    while (p != NULL)
      {
          ++p;
          if (strncmp(p, "Buffers:", 8) == 0)
            {
                SKIP_TOKEN(p);
                GET_NEXT_VALUE(p, q, values[i_membuffers], length,
                               "Buffers not found", ' ');
            }
          else if (strncmp(p, "Cached:", 7) == 0)
            {
                SKIP_TOKEN(p);
                GET_NEXT_VALUE(p, q, values[i_memcached], length,
                               "Cached not found", ' ');
            }
          else if (strncmp(p, "MemFree:", 8) == 0)
            {
                SKIP_TOKEN(p);
                memfree = strtoul(p, &p, 10);
                snprintf(values[i_memused], BIGINT_LEN, "%lu",
                         memtotal - memfree);
                snprintf(values[i_memfree], BIGINT_LEN, "%lu", memfree);
            }
          else if (strncmp(p, "MemShared:", 10) == 0)
            {
                SKIP_TOKEN(p);
                GET_NEXT_VALUE(p, q, values[i_memshared], length,
                               "MemShared not found", ' ');
            }
          else if (strncmp(p, "MemTotal:", 9) == 0)
            {
                SKIP_TOKEN(p);
                memtotal = strtoul(p, &p, 10);
                elog(DEBUG5, "pg_memusage: MemTotal = %lu", memtotal);
            }
          else if (strncmp(p, "SwapFree:", 9) == 0)
            {
                SKIP_TOKEN(p);
                swapfree = strtoul(p, &p, 10);
                snprintf(values[i_swapused], BIGINT_LEN, "%lu",
                         swaptotal - swapfree);
                snprintf(values[i_swapfree], BIGINT_LEN, "%lu", swapfree);
            }
          else if (strncmp(p, "SwapCached:", 11) == 0)
            {
                SKIP_TOKEN(p);
                GET_NEXT_VALUE(p, q, values[i_swapcached], length,
                               "SwapCached not found", ' ');
            }
          else if (strncmp(p, "SwapTotal:", 10) == 0)
            {
                SKIP_TOKEN(p);
                swaptotal = strtoul(p, &p, 10);
                elog(DEBUG5, "pg_memusage: SwapTotal = %lu", swaptotal);
            }
          p = strchr(p, '\n');
      }
#endif                          /* __linux__ */

    elog(DEBUG5, "pg_memusage: [%d] Buffers = %s", (int) i_membuffers,
         values[i_membuffers]);
    elog(DEBUG5, "pg_memusage: [%d] Cached = %s", (int) i_memcached,
         values[i_memcached]);
    elog(DEBUG5, "pg_memusage: [%d] MemFree = %s", (int) i_memfree,
         values[i_memfree]);
    elog(DEBUG5, "pg_memusage: [%d] MemUsed = %s", (int) i_memused,
         values[i_memused]);
    elog(DEBUG5, "pg_memusage: [%d] MemShared = %s", (int) i_memshared,
         values[i_memshared]);
    elog(DEBUG5, "pg_memusage: [%d] SwapCached = %s", (int) i_swapcached,
         values[i_swapcached]);
    elog(DEBUG5, "pg_memusage: [%d] SwapFree = %s", (int) i_swapfree,
         values[i_swapfree]);
    elog(DEBUG5, "pg_memusage: [%d] SwapUsed = %s", (int) i_swapused,
         values[i_swapused]);

    return 1;
}
