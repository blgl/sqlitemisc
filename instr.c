#include <string.h>
#include <sqlite3ext.h>

SQLITE_EXTENSION_INIT1

static char const confused[]    = "SQLite is confused";
static char malformed_8[]       = "malformed UTF-8 text";
static char malformed_16[]      = "malformed UTF-16 text";

/*
 * upgrade this to size_t if SQLite ever gets
 * sqlite3_value_bytes_64 and sqlite3_value_bytes16_64...
 */
typedef unsigned int vsize;

typedef vsize bmh_skips[256];

static void fbmh_setup(
    unsigned char const *needle,
    vsize needlesize,
    vsize mask,
    bmh_skips skips)
{
    unsigned int c;
    vsize ix, limit;

    for (c = 0; c<256; c++) {
        skips[c] = needlesize;
    }
    limit = needlesize-1;
    for (ix = 0; ix<limit; ix++) {
        skips[needle[ix]] = limit-ix;
    }
    if (mask) {
        for (c = 0; c<256; c++) {
            skips[c] = skips[c]+mask & ~mask;
        }
    }
}

static void rbmh_setup(
    unsigned char const *needle,
    vsize needlesize,
    vsize mask,
    bmh_skips skips)
{
    unsigned int c;
    vsize ix, limit;

    for (c = 0; c<256; c++) {
        skips[c] = needlesize;
    }
    limit = needlesize-1;
    for (ix = limit; ix>0; ix--) {
        skips[needle[ix]] = ix;
    }
    if (mask) {
        for (c = 0; c<256; c++) {
            skips[c] = skips[c]+mask & ~mask;
        }
    }
}

#define UTF8_ADVANCE(ptr, size, cp) \
    do { \
        unsigned int c0, c1, c2, c3; \
        if (size>=1 && (c0 = ptr[0])<0x80) { \
            cp = c0; \
            ptr++; \
            size--; \
        } else if (size>=2 && c0>=0xC2 && c0<0xE0 \
                   && (c1 = ptr[1])>=0x80 && c1<0xC0) { \
            cp = c0<<6 & 0x7C0 | c1&0x3F; \
            ptr += 2; \
            size -= 2; \
        } else if (size>=3 && c0>=0xE0 && c0<0xF0 \
                   && (c1 = ptr[1])>=0x80 && c1<0xC0 \
                   && (c2 = ptr[2])>=0x80 && c2<0xC0 \
                   && (cp = c0<<12&0xF000 | c1<<6&0xFC0 | c2&0x3F)>=0x800 \
                   && (cp<0xD800 || cp>=0xE000)) { \
            ptr += 3; \
            size -= 3; \
        } else if (size>=4 && c0>=0xF0 && c0<0xF5 \
                   && (c1 = ptr[1])>=0x80 && c1<0xC0 \
                   && (c2 = ptr[2])>=0x80 && c2<0xC0 \
                   && (c3 = ptr[3])>=0x80 && c3<0xC0 \
                   && (cp = c0<<18&0x1C0000 | c1<<12&0x3F000 \
                       | c2<<6&0xFC0 | c3&0x3F)>=0x10000 \
                   && cp<0x110000) { \
            ptr += 4; \
            size -= 4; \
        } else { \
            cp = -1; \
        } \
    } while (0)

#define UTF8_RETREAT(ptr, size, cp) \
    do { \
        unsigned int c1, c2, c3, c4; \
        if (size>=1 && (c1 = ptr[-1])<0x80) { \
            cp = c1; \
            ptr -= 1; \
            size -= 1; \
        } else if (size>=2 && c1>=0x80 && c1<0xC0 \
                   && (c2 = ptr[-2])>=0xC2 && c2<0xE0) { \
            cp = c2<<6 & 0x7C0 | c1&0x3F; \
            ptr -= 2; \
            size -= 2; \
        } else if (size>=3 && c1>=0x80 && c1<0xC0 \
                   && c2>=0xC2 && c2<0xE0 \
                   && (c3 = ptr[-3])>=0xE0 && c3<0xF0 \
                   && (cp = c3<<12&0xF000 | c2<<6&0xFC0 | c1&0x3F)>=0x800 \
                   && (cp<0xD800 || cp>=0xE000)) { \
            ptr -= 3; \
            size -= 3; \
        } else if (size>=4 && c1>=0x80 && c1<0xC0 \
                   && c2>=0xC2 && c2<0xE0 \
                   && c3>=0xC2 && c3<0xE0 \
                   && (c4 = ptr[-4])>=0xF0 && c4<0xF5 \
                   && (cp = c4<<18&0x1C0000 | c3<<12&0x3F000 \
                       | c2<<6&0xFC0 | c1&0x3F)>=0x10000 \
                   && cp<0x110000) { \
            ptr -= 4; \
            size -= 4; \
        } else { \
            cp = -1; \
        } \
    } while (0)

#define UTF16_ADVANCE(ptr, size, cp) \
    do { \
        unsigned int c0, c1; \
        if (size>=2 && ((c0 = ptr[0])<0xD800 || c0>=0xE000)) { \
            cp = c0; \
            ptr += 1; \
            size -= 2; \
        } else if (size>=4 && c0>=0xD800 && c0<0xDC00 \
                   && (c1 = ptr[1])>=0xDC00 && c1<0xE000) { \
            cp = (c0+0x40)<<10&0x1FFC00 | c1&0x3FF; \
            ptr += 2; \
            size -= 4; \
        } else { \
            cp = -1; \
        } \
    } while (0)

#define UTF16_RETREAT(ptr, size, cp) \
    do { \
        unsigned int c1, c2; \
        if (size>=2 && ((c1 = ptr[-1])<0xD800 || c1>=0xE000)) { \
            cp = c1; \
            ptr -= 1; \
            size -= 2; \
        } else if (size>=4 && c1>=0xDC00 && c1<0xE000 \
                   && (c2 = ptr[-2])>=0xD800 && c2<0xDC00) { \
            cp = (c2+0x40)<<10&0x1FFC00 | c1&0x3FF; \
            ptr += 2; \
            size -= 4; \
        } else { \
            cp = -1; \
        } \
    } while (0)

static sqlite3_int64 instr_blob(
    unsigned char const *haystack,
    vsize stacksize,
    unsigned char const *needle,
    vsize needlesize,
    sqlite3_int64 start)
{
    bmh_skips skips;
    sqlite3_int64 found;

    if (start>1) {
        found = start;
        start--;
        if (start>(sqlite3_int64)stacksize)
            return 0;
        haystack += start;
        stacksize -= start;
    } else {
        found = 1;
    }
    if (needlesize>stacksize)
        return 0;
    if (needlesize<=0)
        return found;
    if (needlesize>1) {
        fbmh_setup(needle, needlesize, 0, skips);
        while (stacksize>=needlesize) {
            vsize skip;

            if (!memcmp(haystack, needle, needlesize))
                return found;
            skip = skips[haystack[needlesize-1]];
            haystack += skip;
            stacksize -= skip;
            found += skip;
        }
    } else {
        unsigned int first;

        first = needle[0];
        while (stacksize>0) {
            if (haystack[0]==first)
                return found;
            haystack++;
            stacksize--;
            found++;
        }
    }
    return 0;
}

static sqlite3_int64 instr_utf8(
    unsigned char const *haystack,
    vsize stacksize,
    unsigned char const *needle,
    vsize needlesize,
    sqlite3_int64 start)
{
    bmh_skips skips;
    sqlite3_int64 found;
    int codepoint;

    if (needlesize>stacksize)
        return 0;
    found = 1;
    while (found<start && stacksize>needlesize) {
        UTF8_ADVANCE(haystack, stacksize, codepoint);
        if (codepoint==-1)
            return -1;
        found++;
    }
    if (found<start)
        return 0;
    if (needlesize<=0)
        return found;
    if (needlesize>1) {
        unsigned char const *next;

        fbmh_setup(needle, needlesize, 0, skips);
        next = haystack;
        while (stacksize>=needlesize) {
            if (haystack>=next) {
                vsize skip;

                if (!memcmp(haystack, needle, needlesize))
                    return found;
                skip = skips[haystack[needlesize-1]];
                if (stacksize-skip<needlesize)
                    return 0;
                next = haystack+skip;
            }
            UTF8_ADVANCE(haystack, stacksize, codepoint);
            if (codepoint==-1)
                return -1;
            found++;
        }
    } else {
        unsigned int first;

        first = needle[0];
        while (stacksize>0) {
            if (haystack[0]==first)
                return found;
            UTF8_ADVANCE(haystack, stacksize, codepoint);
            if (codepoint==-1)
                return -1;
            found++;
        }
    }
    return 0;
}

static sqlite3_int64 instr_utf16(
    unsigned short const *haystack,
    vsize stacksize,
    unsigned short const *needle,
    vsize needlesize,
    sqlite3_int64 start)
{
    bmh_skips skips;
    sqlite3_int64 found;
    int codepoint;

    if (needlesize>stacksize)
        return 0;
    found = 1;
    while (found<start && stacksize>needlesize) {
        UTF16_ADVANCE(haystack, stacksize, codepoint);
        if (codepoint==-1)
            return -1;
        found++;
    }
    if (found<start)
        return 0;
    if (needlesize<=0)
        return found;
    if (needlesize>2) {
        unsigned short const *next;

        fbmh_setup((unsigned char const *)needle, needlesize, 1, skips);
        next = haystack;
        while (stacksize>=needlesize) {
            if (haystack>=next) {
                vsize skip;

                if (!memcmp(haystack, needle, needlesize))
                    return found;
                skip = skips[((unsigned char const *)haystack)[needlesize-1]];
                if (stacksize-skip<needlesize)
                    return 0;
                next = haystack+skip/2;
            }
            UTF16_ADVANCE(haystack, stacksize, codepoint);
            if (codepoint==-1)
                return -1;
            found++;
        }
    } else {
        unsigned int first;

        first = needle[0];
        while (stacksize>0) {
            if (haystack[0]==first)
                return found;
            UTF16_ADVANCE(haystack, stacksize, codepoint);
            if (codepoint==-1)
                return -1;
            found++;
        }
    }
    return 0;
}

static void instr_func(
    sqlite3_context *context,
    int argc,
    sqlite3_value **args)
{
    int stacktype, needletype;
    void const *haystack, *needle;
    vsize stacksize, needlesize;
    sqlite3_int64 start, result;
    char const *malformed;

    if (argc<2)
        goto confused;
    stacktype = sqlite3_value_type(args[0]);
    if (stacktype==SQLITE_NULL)
        return;
    needletype = sqlite3_value_type(args[1]);
    if (needletype==SQLITE_NULL)
        return;
    if (argc>=3) {
        if (sqlite3_value_type(args[2])==SQLITE_NULL)
            return;
        start = sqlite3_value_int64(args[2]);
    } else {
        start = 1;
    }
    malformed = sqlite3_user_data(context);
    if (stacktype==SQLITE_BLOB && needletype==SQLITE_BLOB) {
        haystack = sqlite3_value_blob(args[0]);
        stacksize = sqlite3_value_bytes(args[0]);
        if (!haystack && stacksize>0)
            goto nomem;
        needle = sqlite3_value_blob(args[1]);
        needlesize = sqlite3_value_bytes(args[1]);
        if (!needle && needlesize>0)
            goto nomem;
        result = instr_blob(haystack, stacksize, needle, needlesize, start);
    } else if (malformed==malformed_8) {
        haystack = sqlite3_value_text(args[0]);
        if (!haystack)
            goto nomem;
        stacksize = sqlite3_value_bytes(args[0]);
        needle = sqlite3_value_text(args[1]);
        if (!needle)
            goto nomem;
        needlesize = sqlite3_value_bytes(args[1]);
        result = instr_utf8(haystack, stacksize, needle, needlesize, start);
    } else if (malformed==malformed_16) {
        haystack = sqlite3_value_text16(args[0]);
        if (!haystack)
            goto nomem;
        stacksize = sqlite3_value_bytes16(args[0])&~(vsize)1;
        needle = sqlite3_value_text16(args[1]);
        if (!needle)
            goto nomem;
        needlesize = sqlite3_value_bytes16(args[1])&~(vsize)1;
        result = instr_utf16(haystack, stacksize, needle, needlesize, start);
    } else {
        goto confused;
    }
    if (result<0) {
        sqlite3_result_error(context, malformed, -1);
    } else {
        sqlite3_result_int64(context, result);
    }
    return;

confused:
    sqlite3_result_error(context, confused, sizeof confused-1);
    return;

nomem:
    sqlite3_result_error_nomem(context);
}

static sqlite3_int64 rinstr_blob(
    unsigned char const *haystack,
    vsize stacksize,
    unsigned char const *needle,
    vsize needlesize,
    sqlite3_int64 start)
{
    bmh_skips skips;
    sqlite3_int64 found;

    if (start<=0)
        return 0;
    if (needlesize>stacksize)
        return 0;
    if (start>1) {
        if (start-1>(sqlite3_int64)(stacksize-needlesize))
            start = stacksize-needlesize+1;
        found = start;
        stacksize = start-1;
        haystack += stacksize;
    } else {
        found = 1;
        stacksize = 0;
    }
    if (needlesize<=0)
        return found;
    if (needlesize>1) {
        rbmh_setup(needle, needlesize, 0, skips);
        for (;;) {
            vsize skip;

            if (!memcmp(haystack, needle, needlesize))
                return found;
            skip = skips[haystack[0]];
            if (skip>stacksize)
                return 0;
            haystack -= skip;
            stacksize -= skip;
            found -= skip;
        }
    } else {
        unsigned int first;

        first = needle[0];
        for (;;) {
            if (haystack[0]==first)
                return found;
            if (stacksize<=0)
                return 0;
            haystack--;
            stacksize--;
            found--;
        }
    }
}

static sqlite3_int64 rinstr_utf8(
    unsigned char const *haystack,
    vsize stacksize,
    unsigned char const *needle,
    vsize needlesize,
    sqlite3_int64 start)
{
    bmh_skips skips;
    unsigned char const *haystart = haystack;
    sqlite3_int64 found;
    int codepoint;

    if (start<=0)
        return 0;
    if (needlesize>stacksize)
        return 0;
    found = 1;
    while (found<start && stacksize>needlesize) {
        unsigned char const *oldhaystack = haystack;
        vsize oldstacksize = stacksize;

        UTF8_ADVANCE(haystack, stacksize, codepoint);
        if (codepoint==-1)
            return -1;
        if (stacksize<needlesize) {
            haystack = oldhaystack;
            stacksize = oldstacksize;
            break;
        }
        found++;
    }
    if (needlesize<=0)
        return found;
    stacksize = haystack-haystart;
    if (needlesize>1) {
        unsigned char const *next;

        rbmh_setup(needle, needlesize, 0, skips);
        next = haystack;
        for (;;) {
            if (haystack<=next) {
                vsize skip;

                if (!memcmp(haystack, needle, needlesize))
                    return found;
                skip = skips[haystack[0]];
                if (skip>stacksize)
                    return 0;
                next = haystack-skip;
            }
            if (stacksize<=0)
                return 0;
            UTF8_RETREAT(haystack, stacksize, codepoint);
            found--;
        }
    } else {
        unsigned int first;

        first = needle[0];
        for (;;) {
            if (haystack[0]==first)
                return found;
            if (stacksize<=0)
                return 0;
            UTF8_RETREAT(haystack, stacksize, codepoint);
            found--;
        }
    }
}

static sqlite3_int64 rinstr_utf16(
    unsigned short const *haystack,
    vsize stacksize,
    unsigned short const *needle,
    vsize needlesize,
    sqlite3_int64 start)
{
    bmh_skips skips;
    unsigned short const *haystart = haystack;
    sqlite3_int64 found;
    int codepoint;

    if (start<=0)
        return 0;
    if (needlesize>stacksize)
        return 0;
    found = 1;
    while (found<start && stacksize>needlesize) {
        unsigned short const *oldhaystack = haystack;
        vsize oldstacksize = stacksize;

        UTF16_ADVANCE(haystack, stacksize, codepoint);
        if (codepoint==-1)
            return -1;
        if (stacksize<needlesize) {
            haystack = oldhaystack;
            stacksize = oldstacksize;
            break;
        }
        found++;
    }
    if (needlesize<=0)
        return found;
    stacksize = (haystack-haystart)*2;
    if (needlesize>2) {
        unsigned short const *next;

        rbmh_setup((unsigned char const *)needle, needlesize, 1, skips);
        next = haystack;
        for (;;) {
            if (haystack<=next) {
                vsize skip;

                if (!memcmp(haystack, needle, needlesize))
                    return found;
                skip = skips[((unsigned char const *)haystack)[0]];
                if (skip>stacksize)
                    return 0;
                next = haystack-skip/2;
            }
            if (stacksize<=0)
                return 0;
            UTF16_RETREAT(haystack, stacksize, codepoint);
            found--;
        }
    } else {
        unsigned int first;

        first = needle[0];
        for (;;) {
            if (haystack[0]==first)
                return found;
            if (stacksize<=0)
                return 0;
            UTF16_RETREAT(haystack, stacksize, codepoint);
            found--;
        }
    }
}

static void rinstr_func(
    sqlite3_context *context,
    int argc,
    sqlite3_value **args)
{
    int stacktype, needletype;
    void const *haystack, *needle;
    vsize stacksize, needlesize;
    sqlite3_int64 start, result;
    char const *malformed;

    if (argc<2)
        goto confused;
    stacktype = sqlite3_value_type(args[0]);
    if (stacktype==SQLITE_NULL)
        return;
    needletype = sqlite3_value_type(args[1]);
    if (needletype==SQLITE_NULL)
        return;
    if (argc>=3) {
        if (sqlite3_value_type(args[2])==SQLITE_NULL)
            return;
        start = sqlite3_value_int64(args[2]);
    } else {
        start = 0x7FFFFFFFFFFFFFFFLL;
    }
    malformed = sqlite3_user_data(context);
    if (stacktype==SQLITE_BLOB && needletype==SQLITE_BLOB) {
        haystack = sqlite3_value_blob(args[0]);
        stacksize = sqlite3_value_bytes(args[0]);
        if (!haystack && stacksize>0)
            goto nomem;
        needle = sqlite3_value_blob(args[1]);
        needlesize = sqlite3_value_bytes(args[1]);
        if (!needle && needlesize>0)
            goto nomem;
        result = rinstr_blob(haystack, stacksize, needle, needlesize, start);
    } else if (malformed==malformed_8) {
        haystack = sqlite3_value_text(args[0]);
        if (!haystack)
            goto nomem;
        stacksize = sqlite3_value_bytes(args[0]);
        needle = sqlite3_value_text(args[1]);
        if (!needle)
            goto nomem;
        needlesize = sqlite3_value_bytes(args[1]);
        result = rinstr_utf8(haystack, stacksize, needle, needlesize, start);
    } else if (malformed==malformed_16) {
        haystack = sqlite3_value_text16(args[0]);
        if (!haystack)
            goto nomem;
        stacksize = sqlite3_value_bytes16(args[0])&~(vsize)1;
        needle = sqlite3_value_text16(args[1]);
        if (!needle)
            goto nomem;
        needlesize = sqlite3_value_bytes16(args[1])&~(vsize)1;
        result = rinstr_utf16(haystack, stacksize, needle, needlesize, start);
    } else {
        goto confused;
    }
    if (result<0) {
        sqlite3_result_error(context, malformed, -1);
    } else {
        sqlite3_result_int64(context, result);
    }
    return;

confused:
    sqlite3_result_error(context, confused, sizeof confused-1);
    return;

nomem:
    sqlite3_result_error_nomem(context);
}

typedef struct funcspec {
    char const *name;
    void (*impl)(
        sqlite3_context *context,
        int argc,
        sqlite3_value **args);
} funcspec;

typedef struct encspec {
    int rep;
    void *ptr;
} encspec;

static funcspec const funcs[2] =
{
    {"instr",  instr_func},
    {"rinstr", rinstr_func}
};

static encspec const encs[2] =
{
    {
        SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS,
        malformed_8
    },
    {
        SQLITE_UTF16 | SQLITE_DETERMINISTIC | SQLITE_INNOCUOUS,
        malformed_16
    }
};

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_extension_init(
    sqlite3 *db,
    char **errmsgOut,
    sqlite3_api_routines const *api)
{
    int funcix, argc, encix;
    int status;

    SQLITE_EXTENSION_INIT2(api);

    for (funcix = 0; funcix<2; funcix++) {
        for (argc = 2; argc<=3; argc++) {
            for (encix = 0; encix<2; encix++) {
                status = sqlite3_create_function(
                    db,
                    funcs[funcix].name,
                    argc,
                    encs[encix].rep,
                    encs[encix].ptr,
                    funcs[funcix].impl,
                    0,
                    0);
                if (status!=SQLITE_OK)
                    goto bail;
            }
        }
    }

bail:
    if (status!=SQLITE_OK && status!=SQLITE_OK_LOAD_PERMANENTLY)
        *errmsgOut = sqlite3_mprintf("%s", sqlite3_errstr(status));
    return status;
}

