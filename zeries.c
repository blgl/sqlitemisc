/*
  generate_zeries -- incompatible alternative to generate_series

  A table-valued function with one result column (value)
  and two optional parameters (step, base).  Parameter values
  must be losslessly convertible to integers.

  The sign of the step is ignored.  The magnitude must be
  greater than 0 and less than 2**63.  The default step is 1.

  The base (default 0) is used to shift the output sequence.
  All output values are congruent to the base (modulo the step).
  That is, the values are a subrange of this infinite sequence:
      ..., base-step*2, base-step, base, base+step, base+step*2, ...

  There are no explicit start or stop parameters; use constraints
  on the value column instead. These constraint types are recognised:
      <  <=  =  IS  >=  >
  All value constraints are processed; if there's a contradiction,
  no rows are returned.

  If you need to count backwards, include an "ORDER BY value DESC"
  clause or something equivalent.

  Example query:
      SELECT value FROM generate_zeries(-3,10)
          WHERE value BETWEEN -9 AND 9;
  Result:
      +-------+
      | value |
      +-------+
      |    -8 |
      |    -5 |
      |    -2 |
      |     1 |
      |     4 |
      |     7 |
      +-------+

 */

#include <string.h>

#include <sqlite3ext.h>

SQLITE_EXTENSION_INIT1

#if SQLITE_VERSION_NUMBER>=3044000
  #define MODULE_VERSION 4
#elif SQLITE_VERSION_NUMBER>=3026000
  #define MODULE_VERSION 3
#elif SQLITE_VERSION_NUMBER>=3007007
  #define MODULE_VERSION 2
#else
  #define MODULE_VERSION 1
#endif


/*
  Avoiding signed integer overflow during wide-range arithmetic
  gets tricky.  Helper functions to the rescue!
*/

#define MAX64 9223372036854775807
#define MIN64 (-MAX64-1)

static sqlite3_uint64 udiff(
    sqlite3_int64 high,
    sqlite3_int64 low)
{
    if (low>=0 || high<0) {
        return (sqlite3_uint64)(high-low);
    } else {
        return (sqlite3_uint64)high+(sqlite3_uint64)-low;
    }
}

static sqlite3_int64 uadd(
    sqlite3_int64 base,
    sqlite3_uint64 diff)
{
    while (diff>MAX64) {
        base+=MAX64;
        diff-=MAX64;
    }
    return base+diff;
}

static sqlite3_int64 usub(
    sqlite3_int64 base,
    sqlite3_uint64 diff)
{
    while (diff>MAX64) {
        base-=MAX64;
        diff-=MAX64;
    }
    return base-diff;
}

static char const schema[] =
    "create table generate_zeries(\n"
    "    value integer,\n"
    "    step integer hidden,\n"
    "    base integer hidden\n"
    ");\n";

static int zeries_connect(
    sqlite3 *db,
    void *data,
    int argc,
    char const * const *argv,
    sqlite3_vtab **vtab_out,
    char **errmsg_out)
{
    int status;
    sqlite3_vtab *vtab;

    (void)data;
    (void)argc;
    (void)argv;
    (void)errmsg_out;
    status=sqlite3_declare_vtab(db,schema);
    if (status!=SQLITE_OK)
        return status;
    status=sqlite3_vtab_config(db,SQLITE_VTAB_INNOCUOUS);
    if (status!=SQLITE_OK)
        return status;
    vtab=sqlite3_malloc(sizeof (sqlite3_vtab));
    if (!vtab)
        return SQLITE_NOMEM;
    *vtab_out=vtab;
    return SQLITE_OK;
}

static int zeries_disconnect(
    sqlite3_vtab *vtab)
{
    sqlite3_free(vtab);
    return SQLITE_OK;
}

enum {
    col_value,
    col_step,
    col_base,

    num_cols,

    col_rowid   = -1
};

typedef struct zeries_cursor {
    sqlite3_vtab_cursor base;
    sqlite3_int64 cols[num_cols];
    sqlite3_int64 step;
    sqlite3_int64 stop;
    char eof;
} zeries_cursor;

static int zeries_open(
    sqlite3_vtab *vtab,
    sqlite3_vtab_cursor **cursor_out)
{
    zeries_cursor *cursor;

    (void)vtab;
    cursor=sqlite3_malloc(sizeof (zeries_cursor));
    if (!cursor)
        return SQLITE_NOMEM;
    cursor->eof=1;
    *cursor_out=&cursor->base;
    return SQLITE_OK;
}

static int zeries_close(
    sqlite3_vtab_cursor *cursor_in)
{
    sqlite3_free(cursor_in);
    return SQLITE_OK;
}

static int zeries_eof(
    sqlite3_vtab_cursor *cursor_in)
{
    zeries_cursor *cursor=(zeries_cursor *)cursor_in;

    return cursor->eof;
}

static int zeries_next(
    sqlite3_vtab_cursor *cursor_in)
{
    zeries_cursor *cursor=(zeries_cursor *)cursor_in;

    if (!cursor->eof) {
        if (cursor->cols[col_value]==cursor->stop) {
            cursor->eof=1;
        } else {
            cursor->cols[col_value]+=cursor->step;
        }
    }
    return SQLITE_OK;
}

static int zeries_column(
    sqlite3_vtab_cursor *cursor_in,
    sqlite3_context *context,
    int col_ix)
{
    zeries_cursor *cursor=(zeries_cursor *)cursor_in;

    if (col_ix==col_rowid)
        col_ix=col_value;
    if (col_ix>=0 && col_ix<num_cols) {
        if (!cursor->eof)
            sqlite3_result_int64(context,cursor->cols[col_ix]);
    } else {
        return SQLITE_INTERNAL;
    }
    return SQLITE_OK;
}

static int zeries_rowid(
    sqlite3_vtab_cursor *cursor_in,
    sqlite_int64 *rowid_out)
{
    zeries_cursor *cursor=(zeries_cursor *)cursor_in;

    *rowid_out=cursor->cols[col_value];
    return SQLITE_OK;
}

/*
  Strategy: build an index string with one letter for each constraint
  that the filter method should process.

  Stick the descending order flag in the index number.
*/

enum {
    constr_offset,
    constr_limit,
    constr_step,
    constr_base,

    num_exact_constraints,

    constr_eq   = num_exact_constraints,
    constr_lt,
    constr_le,
    constr_ge,
    constr_gt
};

enum {
    flag_desc   = 0x01
};

static char const exact_names[num_exact_constraints][7] =
{
    "offset",
    "limit",
    "step",
    "base"
};

static int zeries_best_index(
    sqlite3_vtab *vtab,
    sqlite3_index_info *info)
{
    int ix;
    int arg_ix=0;
    unsigned int flags=0;
    double cost;
    sqlite3_str *index_str;
    int index_num;

    (void)vtab;
    index_str=sqlite3_str_new(NULL);
    index_num=0;
    for (ix=0; ix<info->nConstraint; ix++) {
        struct sqlite3_index_constraint const *constraint;
        int constr_ix=-1;

        constraint=info->aConstraint+ix;
        if (!constraint->usable)
            continue;
        switch (constraint->op) {
        case SQLITE_INDEX_CONSTRAINT_EQ:
        case SQLITE_INDEX_CONSTRAINT_IS:
            switch (constraint->iColumn) {
            case col_rowid:
            case col_value:
                constr_ix=constr_eq;
                break;
            case col_step:
                constr_ix=constr_step;
                break;
            case col_base:
                constr_ix=constr_base;
                break;
            }
            break;

        case SQLITE_INDEX_CONSTRAINT_GE:
            switch (constraint->iColumn) {
            case col_rowid:
            case col_value:
                constr_ix=constr_ge;
                break;
            }
            break;

        case SQLITE_INDEX_CONSTRAINT_GT:
            switch (constraint->iColumn) {
            case col_rowid:
            case col_value:
                constr_ix=constr_gt;
                break;
            }
            break;

        case SQLITE_INDEX_CONSTRAINT_LE:
            switch (constraint->iColumn) {
            case col_rowid:
            case col_value:
                constr_ix=constr_le;
                break;
            }
            break;

        case SQLITE_INDEX_CONSTRAINT_LT:
            switch (constraint->iColumn) {
            case col_rowid:
            case col_value:
                constr_ix=constr_lt;
                break;
            }
            break;

        case SQLITE_INDEX_CONSTRAINT_LIMIT:
            constr_ix=constr_limit;
            break;

        case SQLITE_INDEX_CONSTRAINT_OFFSET:
            constr_ix=constr_offset;
            break;
        }
        if (constr_ix>=0) {
            struct sqlite3_index_constraint_usage *usage;

            usage=info->aConstraintUsage+ix;
            usage->argvIndex=++arg_ix;
            usage->omit=1;
            sqlite3_str_appendchar(index_str,1,'a'+constr_ix);
            flags|=1<<constr_ix;
        }
    }

    if (info->nOrderBy>=1) {
        struct sqlite3_index_orderby const *order;

        for (ix=0; ix<info->nOrderBy; ix++) {
            order=info->aOrderBy+ix;
            if (order->iColumn==col_value || order->iColumn==col_rowid)
                break;
        }
        if (ix<info->nOrderBy) {
            info->orderByConsumed=1;
            if (order->desc)
                index_num|=flag_desc;
        }
    }

    cost=18446744073709551616.0;
    if (flags&(1<<constr_lt|1<<constr_le))
        cost*=0.5;
    if (flags&(1<<constr_ge|1<<constr_gt))
        cost*=0.5;
    if (flags&(1<<constr_eq))
        cost=1.0;
    info->estimatedCost=cost;

    info->idxNum=index_num;
    info->idxStr=sqlite3_str_finish(index_str);
    if (arg_ix>0) {
        if (!info->idxStr)
            return SQLITE_NOMEM;
        info->needToFreeIdxStr=1;
    }

    return SQLITE_OK;
}

static int zeries_filter(
    sqlite3_vtab_cursor *cursor_in,
    int index_num,
    char const *index_str,
    int argc,
    sqlite3_value **argv)
{
    zeries_cursor *cursor=(zeries_cursor *)cursor_in;
    sqlite3_int64 exact[num_exact_constraints];
    sqlite3_int64 offset,limit,step,base;
    sqlite3_int64 lower,upper,start,stop;
    sqlite3_uint64 ustep,length;
    size_t ix_str_len;
    unsigned int seen=0;
    int arg_ix;

    exact[constr_offset]=0;
    exact[constr_limit]=-1;
    exact[constr_step]=1;
    exact[constr_base]=0;
    lower=MIN64;
    upper=MAX64;
    if (index_str) {
        ix_str_len=strlen(index_str);
    } else {
        ix_str_len=0;
    }
    if (argc<0 || (size_t)argc!=ix_str_len)
        return SQLITE_INTERNAL;

    for (arg_ix=0; arg_ix<argc; arg_ix++) {
        sqlite3_value *val;
        double dval;
        sqlite3_int64 ival;
        int ntype;
        int constr_ix;
        unsigned int constr_flag;

        val=argv[arg_ix];
        constr_ix=index_str[arg_ix]-'a';
        ntype=sqlite3_value_numeric_type(val);
        switch (ntype) {
        case SQLITE_INTEGER:
            ival=sqlite3_value_int64(val);
            break;
        case SQLITE_FLOAT:
            dval=sqlite3_value_double(val);
            ival=dval;
            break;
        }

        switch (constr_ix) {
        case constr_offset:
        case constr_limit:
        case constr_step:
        case constr_base:
            switch (ntype) {
            case SQLITE_INTEGER:
                break;
            case SQLITE_FLOAT:
                if (ival==dval)
                    break;
                /* fall through */
            default:
                if (cursor->base.pVtab->zErrMsg)
                    sqlite3_free(cursor->base.pVtab->zErrMsg);
                cursor->base.pVtab->zErrMsg=
                    sqlite3_mprintf("%s parameter has wrong type",
                                    exact_names[constr_ix]);
                cursor->eof=1;
                return SQLITE_MISMATCH;
            }
            constr_flag=1<<constr_ix;
            if (seen&constr_flag) {
                if (ival!=exact[constr_ix])
                    goto empty;
            } else {
                exact[constr_ix]=ival;
                seen|=constr_flag;
            }
            break;

        case constr_eq:
            switch (ntype) {
            case SQLITE_INTEGER:
                break;
            case SQLITE_FLOAT:
                if (ival==dval)
                    break;
                /* fall through */
            default:
                goto empty;
            }
            if (ival<lower || ival>upper)
                goto empty;
            lower=ival;
            upper=ival;
            break;

        case constr_lt:
            switch (ntype) {
            case SQLITE_FLOAT:
                if (ival>=dval) {
                case SQLITE_INTEGER:    /* Yes, this is excessively clever. */
                    if (ival>MIN64) {
                        ival--;
                    } else {
                        goto empty;
                    }
                }
                break;
            default:
                goto empty;
            }
            if (ival<upper) {
                if (ival<lower)
                    goto empty;
                upper=ival;
            }
            break;

        case constr_le:
            switch (ntype) {
            case SQLITE_INTEGER:
                break;
            case SQLITE_FLOAT:
                if (ival>dval) {
                    if (ival>MIN64) {
                        ival--;
                    } else {
                        goto empty;
                    }
                }
                break;
            default:
                goto empty;
            }
            if (ival<upper) {
                if (ival<lower)
                    goto empty;
                upper=ival;
            }
            break;

        case constr_ge:
            switch (ntype) {
            case SQLITE_INTEGER:
                break;
            case SQLITE_FLOAT:
                if (ival<dval) {
                    if (ival<MAX64) {
                        ival++;
                    } else {
                        goto empty;
                    }
                }
                break;
            default:
                goto empty;
            }
            if (ival>lower) {
                if (ival>upper)
                    goto empty;
                lower=ival;
            }
            break;

        case constr_gt:
            switch (ntype) {
            case SQLITE_FLOAT:
                if (ival<=dval) {
                case SQLITE_INTEGER:
                    if (ival<MAX64) {
                        ival++;
                    } else {
                        goto empty;
                    }
                }
                break;
            default:
                goto empty;
            }
            if (ival>lower) {
                if (ival>upper)
                    goto empty;
                lower=ival;
            }
            break;

        default:
            cursor->eof=1;
            return SQLITE_INTERNAL;
        }
    }

    step=exact[constr_step];
    if (step>0) {
        ustep=step;
    } else if (step<0 && step>MIN64) {
        ustep=-step;
    } else {
        if (cursor->base.pVtab->zErrMsg)
            sqlite3_free(cursor->base.pVtab->zErrMsg);
        cursor->base.pVtab->zErrMsg=
            sqlite3_mprintf("%s","step parameter out of range");
        cursor->eof=1;
        return SQLITE_ERROR;
    }

    base=exact[constr_base];
    if (ustep>1) {
        sqlite3_int64 lowest,highest;

        lowest=usub(base,udiff(base,MIN64)/ustep*ustep);
        if (upper<lowest)
            goto empty;
        upper=uadd(lowest,udiff(upper,lowest)/ustep*ustep);

        highest=uadd(base,udiff(MAX64,base)/ustep*ustep);
        if (lower>highest)
            goto empty;
        lower=usub(highest,udiff(highest,lower)/ustep*ustep);

        if (lower>upper)
            goto empty;
    }

    length=udiff(upper,lower)/ustep;
    offset=exact[constr_offset];
    if (offset>0 && (sqlite3_uint64)offset>length)
        goto empty;
    limit=exact[constr_limit];

    if (!(index_num&flag_desc)) {
        start=lower;
        stop=upper;
        step=ustep;
        if (offset>0) {
            start=uadd(start,(sqlite3_uint64)offset*ustep);
            length-=(sqlite3_uint64)offset;
        }
        if (limit>0 && (sqlite3_uint64)limit<=length)
            stop=uadd(start,(sqlite3_uint64)(limit-1)*ustep);
    } else {
        start=upper;
        stop=lower;
        step=-(sqlite3_int64)ustep;
        if (offset>0) {
            start=usub(start,(sqlite3_uint64)offset*ustep);
            length-=(sqlite3_uint64)offset;
        }
        if (limit>0 && (sqlite3_uint64)limit<=length)
            stop=usub(start,(sqlite3_uint64)(limit-1)*ustep);
    }
    cursor->cols[col_value]=start;
    cursor->cols[col_step]=exact[constr_step];
    cursor->cols[col_base]=exact[constr_base];
    cursor->step=step;
    cursor->stop=stop;
    cursor->eof=0;
    return SQLITE_OK;

empty:
    cursor->eof=1;
    return SQLITE_OK;
}

static sqlite3_module methods =
{
    MODULE_VERSION,
    zeries_connect,     /* xCreate */
    zeries_connect,     /* xConnect */
    zeries_best_index,  /* xBestIndex */
    zeries_disconnect,  /* xDisconnect */
    zeries_disconnect,  /* xDestroy */
    zeries_open,        /* xOpen */
    zeries_close,       /* xClose */
    zeries_filter,      /* xFilter */
    zeries_next,        /* xNext */
    zeries_eof,         /* xEof */
    zeries_column,      /* xColumn */
    zeries_rowid,       /* xRowid */
    0,                  /* xUpdate */
    0,                  /* xBegin */
    0,                  /* xSync */
    0,                  /* xCommit */
    0,                  /* xRollback */
    0,                  /* xFindFunction */
    0,                  /* xRename */
#if MODULE_VERSION>=2
    0,                  /* xSavepoint */
    0,                  /* xRelease */
    0,                  /* xRollbackTo */
#endif
#if MODULE_VERSION>=3
    0,                  /* xShadowName */
#endif
#if MODULE_VERSION>=4
    0,                  /* xIntegrity */
#endif
};

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_zeries_init(
    sqlite3 *db,
    char **errmsg_out,
    sqlite3_api_routines const *api)
{
    int status=SQLITE_OK;

    (void)errmsg_out;
    SQLITE_EXTENSION_INIT2(api);

    if (sqlite3_libversion_number()>=3009000)
        methods.xCreate=0;
    status=sqlite3_create_module_v2(db,"generate_zeries",&methods,NULL,0);

    return status;
}

