#ifndef __TC_ERL_CODEC_H_
#define __TC_ERL_CODEC_H_

#ifdef __cplusplus
extern "C"
    {
#endif

#define DECODE_BINARY(xlen, x)                  \
  do                                            \
    {                                           \
      DECODE_NATIVE_64_UNSIGNED (xlen);         \
      (x) = (xlen > 0) ? buf : NULL;            \
      buf += xlen;                              \
      buflen -= xlen;                           \
    }                                           \
  while (0)

#define DECODE_BYTE(x)                  \
  do                                    \
    {                                   \
      if (buflen < 1) { goto ERROR; }   \
      (x) = *buf;                       \
      buf += 1;                         \
      buflen -= 1;                      \
    }                                   \
  while (0)

#define DECODE_OMODE(x)                                 \
  do                                                    \
    {                                                   \
      uint64_t _omode;                                  \
                                                        \
      DECODE_NATIVE_64_UNSIGNED (_omode);               \
                                                        \
      (x) = (_omode & 1) ? BDBOREADER : 0;              \
      (x) |= (_omode & 2) ? BDBOWRITER : 0;             \
      if (_omode & 2)                                   \
        {                                               \
          (x) |= (_omode & 4) ? BDBOCREAT : 0;          \
          (x) |= (_omode & 8) ? BDBOTRUNC : 0;          \
        }                                               \
      (x) |= (_omode & 16) ? BDBONOLCK : 0;             \
      (x) |= (_omode & 32) ? BDBOLCKNB : 0;             \
    }                                                   \
  while (0)

#define DECODE_NATIVE_64_SIGNED(x)              \
  do                                            \
    {                                           \
      if (buflen < 8) { goto ERROR; }           \
      (x) = * ((int64_t *) buf);                \
      buf += 8;                                 \
      buflen -= 8;                              \
    }                                           \
  while (0)

#define DECODE_NATIVE_64_UNSIGNED(x)            \
  do                                            \
    {                                           \
      if (buflen < 8) { goto ERROR; }           \
      (x) = * ((uint64_t *) buf);               \
      buf += 8;                                 \
      buflen -= 8;                              \
    }                                           \
  while (0)

#define DECODE_STRING(x)                        \
  do                                            \
    {                                           \
      uint64_t len;                             \
      DECODE_BINARY (len, x);                   \
    }                                           \
  while (0)

#define DECODE_TUNE_OPTS(x)                             \
  do                                                    \
    {                                                   \
      uint64_t _extra;                                  \
                                                        \
      DECODE_NATIVE_64_UNSIGNED (_extra);               \
                                                        \
      (x) = (_extra & 1) ? BDBTLARGE : 0;               \
      (x) |= (_extra & 2) ? BDBTDEFLATE: 0;             \
      (x) |= (_extra & 4) ? BDBTTCBS : 0;               \
    }                                                   \
  while (0)

#ifdef __cplusplus
    }
#endif

#endif /* __TC_ERL_CODEC_H_ */
