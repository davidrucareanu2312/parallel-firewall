/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_CONSUMER_H__
#define __SO_CONSUMER_H__

#include "ring_buffer.h"
#include "packet.h"

struct entry_t {
    char *buf;
	ssize_t buf_size;
    int timestamp;
};

struct pq_t {
	struct entry_t **pq_data;
	int pq_size;
	int pq_cap;
};

typedef struct so_consumer_ctx_t {
	struct so_ring_buffer_t *producer_rb;

    /* TODO: add synchronization primitives for timestamp ordering */
	unsigned int fd;

	pthread_mutex_t mutex;

	pthread_cond_t timestamp_ord_cond;

	struct pq_t *pq;

	int num_consumers;

	int num_max;
} so_consumer_ctx_t;

int create_consumers(pthread_t *tids,
					int num_consumers,
					so_ring_buffer_t *rb,
					const char *out_filename);

#endif /* __SO_CONSUMER_H__ */
