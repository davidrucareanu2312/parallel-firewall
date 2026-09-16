// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	/* TODO: implement ring_buffer_init */
	(void) ring;
	(void) cap;

	if (cap == 0)
		return -1;

	ring->cap = cap;
	ring->len = 0;

	ring->read_pos = 0;
	ring->write_pos = 0;

	ring->ring_stop = 0;

	ring->data = (char *)malloc(cap * sizeof(char));

	pthread_mutex_init(&ring->ring_mutex, NULL);

	pthread_cond_init(&ring->ring_empty, NULL);
	pthread_cond_init(&ring->ring_full, NULL);

	return 1;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: implement ring_buffer_enqueue */
	(void) ring;
	(void) data;
	(void) size;

	pthread_mutex_lock(&ring->ring_mutex);

	while (ring->len + size > ring->cap && ring->ring_stop == 0)
		pthread_cond_wait(&ring->ring_full, &ring->ring_mutex);

	size_t cap = ring->cap;
	size_t read = ring->read_pos;
	size_t write = ring->write_pos;

	if (write < read) {
		memcpy(ring->data + write, data, size);
		write += size;
	} else {
		if (write + size < cap) {
			memcpy(ring->data + write, data, size);
			write += size;
		} else {
			int first_half_size = cap - write;
			int second_half_size = size - first_half_size;

			memcpy(ring->data + write, data, first_half_size);
			memcpy(ring->data, data + first_half_size, second_half_size);
			write = second_half_size;
		}
	}

	ring->len = ring->len + size;
	ring->write_pos = write;

	pthread_cond_signal(&ring->ring_empty);

	pthread_mutex_unlock(&ring->ring_mutex);

	return 1;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: Implement ring_buffer_dequeue */
	(void) ring;
	(void) data;
	(void) size;

	if (ring == NULL)
		return 0;

	if (data == NULL)
		return 0;

	pthread_mutex_lock(&ring->ring_mutex);

	while (ring->len == 0 && ring->ring_stop == 0)
		pthread_cond_wait(&ring->ring_empty, &ring->ring_mutex);

	if (ring->len == 0 && ring->ring_stop != 0) {
		pthread_mutex_unlock(&ring->ring_mutex);
		return -1;
	}

	size_t cap = ring->cap;
	size_t read = ring->read_pos;
	size_t write = ring->write_pos;

	if (read < write) {
		memcpy(data, ring->data + read, size);
		read += size;
	} else {
		if (read + size < cap) {
			memcpy(data, ring->data + read, size);
			read += size;
		} else {
			int first_half_size = cap - read;
			int second_half_size = size - first_half_size;

			memcpy(data, ring->data + read, first_half_size);
			memcpy(data + first_half_size, ring->data, second_half_size);

			read = second_half_size;
		}
	}

	ring->len = ring->len - size;
	ring->read_pos = read;

	pthread_cond_signal(&ring->ring_full);

	pthread_mutex_unlock(&ring->ring_mutex);

	return 1;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_destroy */
	(void) ring;

	free(ring->data);
	pthread_mutex_destroy(&ring->ring_mutex);
	pthread_cond_destroy(&ring->ring_empty);
	pthread_cond_destroy(&ring->ring_full);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_stop */
	(void) ring;

	pthread_mutex_lock(&ring->ring_mutex);

	ring->ring_stop = 1;

	pthread_cond_broadcast(&ring->ring_empty);

	pthread_mutex_unlock(&ring->ring_mutex);
}
