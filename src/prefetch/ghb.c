#include "defs.h"
#include "murmur3.h"
#include "ghb.h"
#include "dc.h"


/* -----------------------------------------------------------------------------
 * Private functions
 * -----------------------------------------------------------------------------
 */

static inline
uint64_t compute_tag(uint8_t ctx, uint64_t lba) {
  /* A 'concentration zone' (czone) is a contiguous range of LBAs,
   * assumed to have correlated access behavior.
   * The constant below corresponds to a 1 GB czone, assuming 512B sectors.
   */
#define CTX_SHIFT		(56)
#define CZONE_SIZE_SHIFT	(21)

  uint64_t czone = (lba >> CZONE_SIZE_SHIFT);
  return ((((uint64_t)ctx) << CTX_SHIFT) | czone);
}

static inline
int get_index(ghb_t *ghb, uint8_t ctx, uint64_t lba) {
  uint64_t hash[2], h;
  uint64_t tag_buf = compute_tag(ctx, lba);
  
  MurmurHash3_x86_128(&tag_buf, sizeof(uint64_t), 42, hash);
  h = (hash[0] ^ hash[1]);
  return (h % ghb->params.n_index);
}

static inline
int add_to_history(ghb_t *ghb, uint8_t ctx, uint64_t lba, int prev_idx) {
  int h_idx;
  ghb_history_t *h_struct;

  /* Identify history_buf entry */
  h_idx = ghb->next_idx;
  if (h_idx < 0) h_idx += ghb->params.n_history;
  h_struct = ghb->history_buf + h_idx;
  /* Update entry */
  h_struct->ctx = ctx;
  h_struct->lba = lba;
  h_struct->prev_idx = prev_idx;
  /* Increment next_idx */
  ghb->next_idx += 1;
  if (ghb->next_idx == ghb->params.n_history) ghb->next_idx = 0;

  /* Return index */
  return h_idx;
}

static inline
bool update_index_and_history(ghb_t *ghb, int idx, uint8_t ctx, uint64_t lba) {
  ghb_index_t *i_struct = ghb->index_tbl + idx;
  int last_idx = -1;

  if (compute_tag(i_struct->ctx, i_struct->last_lba)
      == compute_tag(ctx, lba))
    last_idx = i_struct->last_idx;
  else
    i_struct->ctx = ctx;
  
  i_struct->last_lba = lba;
  i_struct->last_idx = add_to_history(ghb, ctx, lba, last_idx);
  /* true if we added to an existing stream */
  return (last_idx != -1);
}

static inline
int query_history(ghb_t *ghb, int idx, uint64_t *prefetch_lbas) {
  dc_t *dc_struct = (dc_t *)ghb->priv;
  int n_entries = 0;
  ghb_index_t *i_struct = ghb->index_tbl + idx;
  ghb_history_t *h_struct = ghb->history_buf + i_struct->last_idx;
  uint64_t last_lba = i_struct->last_lba;

  dc_reset(dc_struct);

  /* Walk the links backward till we hit one of the following conditions:
     - a prev_idx that indicates start of a new stream => sets h_struct to NULL
     - an h_struct entry that refers to a different ctx
     - we exceed the desired lookback history
  */
  while (h_struct &&
	 (compute_tag(h_struct->ctx, h_struct->lba) == 
	  compute_tag(i_struct->ctx, i_struct->last_lba)) &&
	 (n_entries < ghb->params.n_lookback)) {
    bool done_flag = false;

    n_entries++;
    done_flag = dc_add_record(dc_struct, h_struct->lba);
    if (done_flag) break;
    h_struct = (h_struct->prev_idx != -1)? (ghb->history_buf + h_struct->prev_idx) : NULL;
  }

  return dc_get_requests(dc_struct, last_lba, prefetch_lbas);
}


/* -----------------------------------------------------------------------------
 * Public functions
 * -----------------------------------------------------------------------------
 */

void ghb_init(ghb_t *ghb, ghb_params_t *gparams) {
  memset(ghb, 0, sizeof(ghb_t));
  ghb->params = *gparams;

  /* Index table */
  ghb->index_tbl = (ghb_index_t *)malloc(sizeof(ghb_index_t) * ghb->params.n_index);
  VALIDATE_MALLOC(ghb->index_tbl);
  memset(ghb->index_tbl, 0, sizeof(ghb_index_t)*ghb->params.n_index);

  /* History buffer */
  ghb->history_buf = (ghb_history_t *)malloc(sizeof(ghb_history_t) * ghb->params.n_history);
  VALIDATE_MALLOC(ghb->history_buf);
  memset(ghb->history_buf, 0, sizeof(ghb_history_t)*ghb->params.n_history);

  /* Auxillary structure for DC "delta correlation" prefetching */
  ghb->priv = dc_alloc_and_init(ghb->params.n_lookback, ghb->params.prefetch_depth);

  /* Initialize next_idx
   *   convention: a negative value indicates we have not yet wrapped around
   *               index value is derived by adding n_history to this quantity
   */
  ghb->next_idx = -1 * ghb->params.n_history;
}

void ghb_finalize(ghb_t *ghb) {
  dc_finalize_and_free((dc_t *)ghb->priv);
  free(ghb->history_buf);
  free(ghb->index_tbl);
}

int ghb_update_and_query(ghb_t *ghb, uint8_t ctx, uint64_t lba,
			 uint64_t *prefetch_lbas) {
  int idx = get_index(ghb, ctx, lba);
  if (update_index_and_history(ghb, idx, ctx, lba))
    return query_history(ghb, idx, prefetch_lbas);
  /* default: no prefetch */
  return 0;
}

