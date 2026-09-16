// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

void push_pq(struct pq_t *pq, struct entry_t *entry)
{
	int i = pq->pq_size;

	if (pq->pq_size == pq->pq_cap) {
		pq->pq_cap *= 2;
		pq->pq_data = realloc(pq->pq_data, pq->pq_cap * sizeof(struct entry_t *));
	}
	while (i > 0 && pq->pq_data[i - 1]->timestamp > entry->timestamp) {
		pq->pq_data[i] = pq->pq_data[i - 1];
		i--;
	}

	pq->pq_data[i] = entry;
	pq->pq_size++;
}

struct entry_t *peek_pq(struct pq_t *pq)
{
	if (pq->pq_size == 0)
		return NULL;
	return pq->pq_data[0];
}

void pop_pq(struct pq_t *pq)
{
	if (pq->pq_size == 0)
		return;

	for (int i = 1; i < pq->pq_size; i++)
		pq->pq_data[i - 1] = pq->pq_data[i];

	pq->pq_size--;
}


void *consumer_thread(void *ctx_void)
{
	so_consumer_ctx_t *ctx = (so_consumer_ctx_t *)ctx_void;

	char packet_buf[sizeof(so_packet_t)];

	while (1) {
		int ret = ring_buffer_dequeue(ctx->producer_rb, packet_buf, sizeof(so_packet_t));

		if (ret < 0) {
			pthread_mutex_lock(&ctx->mutex);
			ctx->num_consumers--;
			pthread_cond_broadcast(&ctx->timestamp_ord_cond);
			pthread_mutex_unlock(&ctx->mutex);
			break;
		}

		so_packet_t *pkt = (so_packet_t *)packet_buf;

		struct entry_t *cur_entry = malloc(sizeof(struct entry_t));

		if (!cur_entry)
			continue;

		char *buf = malloc(256);

		unsigned long timestamp = pkt->hdr.timestamp;

		cur_entry->timestamp = timestamp;

		int action_process_packet = process_packet(pkt);

		unsigned long hash = packet_hash(pkt);

		int log_len = snprintf(buf, 256, "%s %016lx %lu\n", RES_TO_STR(action_process_packet), hash, timestamp);

		cur_entry->buf = buf;
		cur_entry->buf_size = (ssize_t)log_len;

		pthread_mutex_lock(&ctx->mutex);

		push_pq(ctx->pq, cur_entry);
		pthread_cond_broadcast(&ctx->timestamp_ord_cond);


		pthread_mutex_unlock(&ctx->mutex);
	}

	return NULL;
}


void *master_thread(void *ctx_void)
{
	so_consumer_ctx_t *ctx = (so_consumer_ctx_t *)ctx_void;

	while (1) {
		pthread_mutex_lock(&ctx->mutex);

		while (ctx->pq->pq_size < 2 * ctx->num_max && ctx->num_consumers > 0)
			pthread_cond_wait(&ctx->timestamp_ord_cond, &ctx->mutex);

		if (ctx->pq->pq_size == 0 && ctx->num_consumers == 0) {
			pthread_mutex_unlock(&ctx->mutex);
			break;
		}

		struct entry_t *entry = peek_pq(ctx->pq);

		if (!entry) {
			pthread_mutex_unlock(&ctx->mutex);
			continue;
		}
		pop_pq(ctx->pq);

		ssize_t written = write(ctx->fd, entry->buf, entry->buf_size);

		(void)written;

		pthread_mutex_unlock(&ctx->mutex);
	}

	while (ctx->pq->pq_size > 0) {
		struct entry_t *entry = peek_pq(ctx->pq);

		pop_pq(ctx->pq);

		ssize_t written = write(ctx->fd, entry->buf, entry->buf_size);

		(void)written;
	}

	return NULL;
}

int create_consumers(pthread_t *tids, int num_consumers,
					 so_ring_buffer_t *rb, const char *out_filename)
{
	int fd = open(out_filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);

	so_consumer_ctx_t *ctx = malloc(sizeof(so_consumer_ctx_t));

	ctx->producer_rb = rb;
	ctx->fd = fd;

	struct pq_t *pq = malloc(sizeof(struct pq_t));

	pq->pq_cap = 8;
	pq->pq_size = 0;
	pq->pq_data = malloc(pq->pq_cap * sizeof(struct entry_t *));
	ctx->pq = pq;

	ctx->num_max = num_consumers;
	ctx->num_consumers = num_consumers;

	pthread_mutex_init(&ctx->mutex, NULL);
	pthread_cond_init(&ctx->timestamp_ord_cond, NULL);

	for (int i = 0; i < num_consumers; i++)
		pthread_create(&tids[i], NULL, consumer_thread, ctx);

	ctx->num_consumers = num_consumers;
	pthread_create(&tids[num_consumers], NULL, master_thread, ctx);

	return num_consumers + 1;
}
