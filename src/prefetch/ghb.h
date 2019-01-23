#ifndef __GHB_H__
#define __GHB_H__

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------------------------------------------------------------
 * Global History Buffer-based Cache Prefetching
 *
 * The implementation is based on the following papers:
 *   "Data Cache Prefetching Using a Global History Buffer",
 *   Nesbit and Smith,
 *   ISCA'2004
 *   "AC/DC: An Adaptive Data Cache Prefetcher",
 *   Nesbit, Dhodapkar, and Smith,
 *   PACT'2004
 *
 * Some ideas also come from:
 *   "Path Confidence Based Lookahead Prefetching",
 *   Kim, Pugsley, Gratz, Reddy, Wilkerson, and Chisthti,
 *   MICRO'2016
 * ------------------------------------------------------------------------------
 */

typedef struct {
  int		n_index;	/* # of entries in index table */
  int		n_history;	/* # of entries in history buffer */
  int		n_lookback;	/* # of entries to look back in a stream */
  int		prefetch_depth;	/* max # of recommendations */
} ghb_params_t;

typedef struct {
  uint8_t	ctx;
  uint64_t	last_lba;
  int		last_idx;	/* Pointer into history buffer */
} ghb_index_t;

typedef struct {
  uint8_t	ctx;
  uint64_t	lba;
  int		prev_idx;
} ghb_history_t;

typedef struct {
  ghb_params_t	params;
  ghb_index_t	*index_tbl;	/* Index table */
  ghb_history_t	*history_buf;	/* History buffer */
  void		*priv;		/* Opaque auxillary structure */
  int		next_idx;	/* Next entry index into history buffer */
} ghb_t;


void ghb_init(ghb_t *ghb, ghb_params_t *gparams);
void ghb_finalize(ghb_t *ghb);

/* Main API call: Updates the GHB structure with the miss LBA,
 * and retrieves recommendation of LBAs to fetch.
 * - prefetch_lbas is assumed to have enough space to hold
 *   max number of recommendations (= prefetch_depth)
 * - return value = # of recommendations
 */
int ghb_update_and_query(ghb_t *ghb, uint8_t ctx, uint64_t lba,
			 uint64_t *prefetch_lbas);
#ifdef __cplusplus
}
#endif

#endif	/* __GHB_H__ */
