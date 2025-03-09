#include "schedy.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/queue.h>
#include <time.h>
#include <unistd.h>

#include "coroed/api/task.h"
#include "coroed/core/relax.h"
#include "coroed/core/spinlock.h"
#include "kthread.h"
#include "uthread.h"

// Add this definition if CLOCK_MONOTONIC is not defined
#ifndef CLOCK_MONOTONIC
#define CLOCK_MONOTONIC 1
#endif

enum {
  /**
   * Максимальное количество файберов, которое может
   * одновременно выполняться на планировщике.
   */
  SCHED_THREADS_LIMIT = 512,

  /**
   * Количество рабочих потоков для исполнения файберов.
   */
  SCHED_WORKERS_COUNT = (size_t)(8),

  /**
   * Обеспечивает костыль для ретрая операций
   * `submit` и `acquire_next` при высокой
   * конкуренции на спинлоках файберов.
   */
  SCHED_NEXT_MAX_ATTEMPTS = (size_t)(16),

  /**
   * Максимальное количество событий, которые могут быть
   * обработаны в одном цикле epoll.
   */
  MAX_EPOLL_EVENTS = (size_t)(64),

  /**
   * Максимальное время ожидания событий в миллисекундах.
   */
  MAX_EPOLL_TIMEOUT = (int)(1000),
};

// Define task priority enum before using it
enum task_priority { PRIORITY_HIGH = 0, PRIORITY_NORMAL = 1, PRIORITY_LOW = 2, PRIORITY_COUNT = 3 };

struct task;
struct worker;

static int sched_loop(void* argument);
static int epoll_loop(void* argument);
void sched_task_init(struct task* task);
void sched_worker_init(struct worker* worker, size_t index);
task_t sched_try_submit(void (*entry)(), void* argument);
void sched_switch_to_scheduler(struct task* task);
void sched_finish(struct task* task);
void update_task_time(struct task* task);
uint64_t get_time_ns();
uint64_t convert_time_to_ns(struct timespec* time);
void sched_enqueue_task(struct task* task);
void sched_dequeue_task(struct task* task);
void sched_adjust_priority(struct task* task);

// Add these global variables for statistics
static int stats_fd = -1;
static const char* STATS_FILENAME = "log/scheduler_stats.log";
static uint64_t last_stats_time = 0;
static const uint64_t STATS_INTERVAL_NS = 1000000000ULL;  // 1 seconds

/**
 * Задача, выполняющаяся на планировщике.
 */
struct task {
  /**
   * Контекст исполнения.
   */
  struct uthread* thread;

  /**
   * Установлен при `UTHREAD_RUNNING`. Указывает на
   * рабочий поток, на котором исполняется задача.
   * Необходим для получения контекста локального
   * планировщика при операциях `yield`, `await`, `submit`.
   */
  struct worker* worker;

  enum {
    /** Готова к исполнению. */
    UTHREAD_RUNNABLE,

    /** Прямо сейчас выполняется. */
    UTHREAD_RUNNING,

    /** Завершена и скоро станет зомби. */
    UTHREAD_FINISHED,

    /** Отработала и может быть переиспользования. */
    UTHREAD_ZOMBIE,

    /** Заблокирована на ввод-вывод. */
    UTHREAD_BLOCKED,
  } state;  // Текущее состояние задачи

  /**
   * Защищает поля структуры от неупорядоченного доступа.
   */
  struct spinlock lock;

  /**
   * Статистика времени в различных состояниях.
   */
  struct timespec last_state_change;  // Время последнего изменения состояния
  uint64_t time_running;              // Всего времени в состоянии `RUNNING`
  uint64_t time_runnable;             // Всего времени в состоянии `RUNNABLE`
  uint64_t time_blocked;              // Всего времени в состоянии `BLOCKED`

  LIST_ENTRY(task) entries;  // Узел двусвязного списка

  // Приоритет задачи
  enum task_priority priority;

  // Время, когда задача начала свою текущую кванту
  uint64_t quantum_start;

  // Количество раз, когда задача была прервана
  uint32_t preemption_count;
};

/**
 * Рабочий поток, исполняющий файберы.
 */
struct worker {
  /**
   * Идентификатор рабочего потока.
   */
  size_t index;

  /**
   * Поток операционной системы, на котором
   * исполняется рабочий.
   */
  struct kthread kthread;

  /**
   * Контекст планировщика. Нужно переключиться на него,
   * чтобы вернуться в планировщик.
   */
  struct uthread sched_thread;

  /**
   * В данный момент исполняемая на рабочем
   * задача. Может быть `NULL`.
   */
  struct task* running_task;

  struct {
    size_t steps;     // Сколько шагов было выполнено
    size_t finished;  // Сколько задач было завершено
  } statistics;       // Локальная статистика работяги
};

static struct spinlock tasks_lock;              // Защищает список задач
static size_t next_task_index = 0;              // Для планирования round-robin
static struct task tasks[SCHED_THREADS_LIMIT];  // Список всех задач

static kthread_id_t kthread_ids[SCHED_WORKERS_COUNT];
static struct worker workers[SCHED_WORKERS_COUNT];

static LIST_HEAD(blocked_task_list, task) blocked_tasks = LIST_HEAD_INITIALIZER(blocked_tasks);
static struct spinlock blocked_lock;

static struct kthread epoll_thread;
static int epoll_fd = -1;

// Приоритетные очереди для разных приоритетов задач
static LIST_HEAD(task_queue, task) ready_queues[PRIORITY_COUNT] = {
    LIST_HEAD_INITIALIZER(ready_queues[0]),
    LIST_HEAD_INITIALIZER(ready_queues[1]),
    LIST_HEAD_INITIALIZER(ready_queues[2])
};

// Спинлоки для каждой приоритетной очереди
static struct spinlock queue_locks[PRIORITY_COUNT];

// Квант (временной срез) в микросекундах для каждого приоритета
static const uint64_t priority_quantum[PRIORITY_COUNT] = {
    10000,  // Высокий приоритет: 10мс
    20000,  // Нормальный приоритет: 20мс
    30000   // Низкий приоритет: 30мс
};

/**
 * Поток epoll, который отслеживает файловые дескрипторы на I/O события.
 */
int epoll_loop(void* argument) {
  (void)argument;

  struct epoll_event events[MAX_EPOLL_EVENTS];

  while (1) {
    // Print statistics periodically
    sched_print_statistics();

    int nfds = epoll_wait(epoll_fd, events, MAX_EPOLL_EVENTS, MAX_EPOLL_TIMEOUT);
    if (nfds == -1) {
      if (errno == EINTR) {
        continue;
      }
      perror("epoll_wait");
      break;
    }

    for (int i = 0; i < nfds; i++) {
      struct task* task = (struct task*)events[i].data.ptr;
      if (task) {
        spinlock_lock(&task->lock);
        if (task->state == UTHREAD_BLOCKED) {
          // Remove from blocked list
          spinlock_lock(&blocked_lock);
          LIST_REMOVE(task, entries);
          spinlock_unlock(&blocked_lock);

          // Boost priority for I/O ready tasks
          if (task->priority > PRIORITY_HIGH) {
            task->priority--;
          }

          // Mark as runnable and add to priority queue
          task->state = UTHREAD_RUNNABLE;
          sched_enqueue_task(task);
        }
        spinlock_unlock(&task->lock);
      }
    }
  }

  return 0;
}

/**
 * Получить текущее время в наносекундах.
 */
uint64_t get_time_ns() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/**
 * Преобразовать время в `struct timespec` в наносекунды.
 */
uint64_t convert_time_to_ns(struct timespec* time) {
  return (uint64_t)time->tv_sec * 1000000000ULL + (uint64_t)time->tv_nsec;
}

/**
 * Обновить параметры времени для задачи.
 */
void update_task_time(struct task* task) {
  uint64_t now = get_time_ns();
  uint64_t delta = now - convert_time_to_ns(&task->last_state_change);

  switch (task->state) {
    case UTHREAD_RUNNING:
      task->time_running += delta;
      break;
    case UTHREAD_RUNNABLE:
      task->time_runnable += delta;
      break;
    case UTHREAD_BLOCKED:
      task->time_blocked += delta;
      break;
    default:
      break;
  }

  clock_gettime(CLOCK_MONOTONIC, &task->last_state_change);
}

/**
 * Установить задачу в пустое состояние.
 */
void sched_task_init(struct task* task) {
  task->thread = NULL;
  task->worker = NULL;
  task->state = UTHREAD_ZOMBIE;
  task->priority = PRIORITY_NORMAL;
  task->preemption_count = 0;
  task->quantum_start = 0;
  spinlock_init(&task->lock);
}

/**
 * Установить рабочего в пустое состояние.
 */
void sched_worker_init(struct worker* worker, size_t index) {
  worker->index = index;
  worker->sched_thread.context = NULL;
  worker->running_task = NULL;
  worker->statistics.steps = 0;
  worker->statistics.finished = 0;
}

void sched_init() {
  spinlock_init(&tasks_lock);
  spinlock_init(&blocked_lock);
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    sched_task_init(&tasks[i]);
  }
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    kthread_ids[i] = 0;
    sched_worker_init(&workers[i], i);
  }

  epoll_fd = epoll_create1(0);
  if (epoll_fd == -1) {
    perror("epoll_create1");
    exit(EXIT_FAILURE);
  }

  // Initialize priority queue locks
  for (size_t i = 0; i < PRIORITY_COUNT; ++i) {
    spinlock_init(&queue_locks[i]);
    LIST_INIT(&ready_queues[i]);
  }

  // Open statistics file with non-blocking flags
  stats_fd = open(STATS_FILENAME, O_WRONLY | O_CREAT | O_APPEND | O_NONBLOCK, 0644);
  if (stats_fd == -1) {
    perror("Failed to open statistics file");
  }

  last_stats_time = get_time_ns();
}

/**
 * Находясь в контексте задачи `task`, переключиться
 * в контекст планировщика.
 */
void sched_switch_to_scheduler(struct task* task) {
  update_task_time(task);
  struct uthread* sched = &task->worker->sched_thread;
  task->worker = NULL;
  uthread_switch(task->thread, sched);
}

/**
 * Находясь в контексте планировщика, переключиться
 * в контекст задачи `task`. Выполнять ее до следующего
 * невынужденного возвращения в планировщик.
 */
void sched_switch_to(struct worker* worker, struct task* task) {
  update_task_time(task);
  assert(task->thread != &worker->sched_thread);

  task->state = UTHREAD_RUNNING;
  task->quantum_start = get_time_ns();

  task->worker = worker;
  worker->running_task = task;

  struct uthread* sched = &worker->sched_thread;
  uthread_switch(sched, task->thread);
}

/**
 * Получить следующую задачу на исполнение в
 * соответствии с текущим алгоритмом планирования.
 *
 * Вызывающему передается владение задачей, а также
 * захваченный лок `task->lock`.
 */
struct task* sched_acquire_next();

/**
 * Вернуть задачу в очередь планирования.
 *
 * Очереди передается владение задачей,
 * а также она отпустит лок `task->lock`.
 */
void sched_release(struct task* task);

/**
 * Цикл планировщика. Выполняется, пока есть задачи.
 */
int sched_loop(void* argument) {
  struct worker* worker = argument;
  kthread_ids[worker->index] = kthread_id();

  for (;;) {
    struct task* task = sched_acquire_next();
    if (task == NULL) {
      break;
    }

    sched_switch_to(worker, task);

    worker->statistics.steps += 1;
    if (task->state == UTHREAD_FINISHED) {
      worker->statistics.finished += 1;
    }

    sched_release(task);
  }

  return 0;
}

struct task* sched_acquire_next() {
  struct task* task = NULL;

  // Try to get a task from each priority queue, starting with highest priority
  for (enum task_priority priority = PRIORITY_HIGH; priority < PRIORITY_COUNT && task == NULL;
       priority++) {
    spinlock_lock(&queue_locks[priority]);

    if (!LIST_EMPTY(&ready_queues[priority])) {
      task = LIST_FIRST(&ready_queues[priority]);
      if (spinlock_try_lock(&task->lock)) {
        if (task->state == UTHREAD_RUNNABLE) {
          LIST_REMOVE(task, entries);
          spinlock_unlock(&queue_locks[priority]);
          return task;
        }
        spinlock_unlock(&task->lock);
      }
    }

    spinlock_unlock(&queue_locks[priority]);
  }

  // If no tasks in priority queues, try the old round-robin approach as fallback
  for (size_t attempt = 0; attempt < SCHED_NEXT_MAX_ATTEMPTS; ++attempt) {
    spinlock_lock(&tasks_lock);  // Protect next_task_index

    for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
      struct task* task = &tasks[next_task_index];
      if (!spinlock_try_lock(&task->lock)) {
        next_task_index = (next_task_index + 1) % SCHED_THREADS_LIMIT;
        continue;
      }

      next_task_index = (next_task_index + 1) % SCHED_THREADS_LIMIT;

      if (task->thread != NULL && task->state == UTHREAD_RUNNABLE) {
        spinlock_unlock(&tasks_lock);
        return task;
      }

      spinlock_unlock(&task->lock);
    }

    spinlock_unlock(&tasks_lock);
    SPINLOOP(2 * attempt);
  }

  return NULL;
}

void sched_release(struct task* task) {
  update_task_time(task);
  task->worker = NULL;

  if (task->state == UTHREAD_FINISHED) {
    // Task is finished, make it a zombie
    uthread_reset(task->thread);
    task->state = UTHREAD_ZOMBIE;
  } else if (task->state == UTHREAD_RUNNING) {
    // Check if task has exceeded its quantum
    uint64_t now = get_time_ns();
    uint64_t elapsed = now - task->quantum_start;
    uint64_t quantum = priority_quantum[task->priority];

    if (elapsed > quantum) {
      // Task has used its quantum, adjust priority and preemption count
      task->preemption_count++;
      sched_adjust_priority(task);
    }

    // Put task back in runnable state and add to appropriate queue
    task->state = UTHREAD_RUNNABLE;
    sched_enqueue_task(task);
  } else if (task->state == UTHREAD_BLOCKED) {
    // Task is blocked, add to blocked list
    spinlock_lock(&blocked_lock);
    LIST_INSERT_HEAD(&blocked_tasks, task, entries);
    spinlock_unlock(&blocked_lock);
  }

  spinlock_unlock(&task->lock);
}

/**
 * Перевести задачу в состояние готовности.
 */
void sched_unblock(struct task* task) {
  spinlock_lock(&blocked_lock);

  if (task->state == UTHREAD_BLOCKED) {
    LIST_REMOVE(task, entries);
    task->state = UTHREAD_RUNNABLE;

    if (task->priority > PRIORITY_HIGH) {
      task->priority--;
    }

    sched_enqueue_task(task);
  }

  spinlock_unlock(&blocked_lock);
}

/**
 * Отметить задачу завершенной.
 */
void sched_finish(struct task* task) {
  task->state = UTHREAD_FINISHED;
}

void task_yield(struct task* caller) {
  sched_switch_to_scheduler(caller);
}

void task_exit(struct task* caller) {
  sched_finish(caller);
  task_yield(caller);
}

task_t task_submit(struct task* caller, uthread_routine entry, void* argument) {
  (void)caller;  // Может быть полезно.
  task_t child = sched_submit(*entry, argument);
  return child;
}

task_t sched_try_submit(void (*entry)(), void* argument) {
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    struct task* task = &tasks[i];

    if (!spinlock_try_lock(&task->lock)) {
      continue;
    }

    if (task->thread == NULL) {
      task->thread = uthread_allocate();
      assert(task->thread != NULL);
      task->state = UTHREAD_ZOMBIE;
    }

    const bool is_submitted = task->state == UTHREAD_ZOMBIE;

    if (task->state == UTHREAD_ZOMBIE) {
      uthread_reset(task->thread);
      uthread_set_entry(task->thread, entry);
      uthread_set_arg_0(task->thread, task);
      uthread_set_arg_1(task->thread, argument);
      task->state = UTHREAD_RUNNABLE;
    }

    spinlock_unlock(&task->lock);
    if (is_submitted) {
      return (task_t){.task = task};
    }
  }

  return (task_t){.task = NULL};
}

task_t sched_submit(void (*entry)(), void* argument) {
  for (size_t attempt = 0; attempt < SCHED_NEXT_MAX_ATTEMPTS; ++attempt) {
    task_t handle = sched_try_submit(entry, argument);
    if (handle.task != NULL) {
      handle.task->priority = PRIORITY_NORMAL;
      sched_enqueue_task(handle.task);
      return handle;
    }
    SPINLOOP(2 * attempt);
  }

  assert(false && "Can't create a task");
}

void sched_start() {
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    enum kthread_status status = kthread_create(&worker->kthread, sched_loop, worker);
    assert(status == KTHREAD_SUCCESS);
  }

  enum kthread_status epoll_status = kthread_create(&epoll_thread, epoll_loop, NULL);
  assert(epoll_status == KTHREAD_SUCCESS);
}

void sched_wait() {
  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    enum kthread_status status = kthread_join(&worker->kthread);
    assert(status == KTHREAD_SUCCESS);
  }
}

char* print_state(int state) {
  switch (state) {
    case UTHREAD_RUNNABLE:
      return "RUNNABLE";
    case UTHREAD_RUNNING:
      return "RUNNING";
    case UTHREAD_FINISHED:
      return "FINISHED";
    case UTHREAD_ZOMBIE:
      return "ZOMBIE";
    case UTHREAD_BLOCKED:
      return "BLOCKED";
    default:
      return "UNKNOWN";
  }
}

// Modified statistics function to use non-blocking I/O
void sched_print_statistics() {
  if (stats_fd == -1)
    return;

  uint64_t now = get_time_ns();

  // Only print statistics periodically
  if (now - last_stats_time < STATS_INTERVAL_NS) {
    return;
  }

  last_stats_time = now;

  // Collect statistics
  size_t tasks_count = 0;
  size_t steps_count = 0;
  uint64_t total_time_running = 0;
  uint64_t total_time_runnable = 0;
  uint64_t total_time_blocked = 0;

  for (size_t i = 0; i < SCHED_WORKERS_COUNT; ++i) {
    struct worker* worker = &workers[i];
    tasks_count += worker->statistics.finished;
    steps_count += worker->statistics.steps;
  }

  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    struct task* task = &tasks[i];
    total_time_running += task->time_running;
    total_time_runnable += task->time_runnable;
    total_time_blocked += task->time_blocked;
  }

  // Format statistics into a buffer
  char buffer[4096];
  int len = snprintf(
      buffer,
      sizeof(buffer),
      "\n--- Scheduler Statistics at %lu ns ---\n"
      "Tasks executed: %zu\n"
      "Total time RUNNING: %lu ns\n"
      "Total time RUNNABLE: %lu ns\n"
      "Total time BLOCKED: %lu ns\n"
      "Steps done: %zu\n",
      now,
      tasks_count,
      total_time_running,
      total_time_runnable,
      total_time_blocked,
      steps_count
  );

  // Add worker statistics
  for (size_t i = 0; i < SCHED_WORKERS_COUNT && len < (int)sizeof(buffer) - 100; ++i) {
    struct worker* worker = &workers[i];
    if (worker->running_task != NULL) {
      len += snprintf(
          buffer + len,
          sizeof(buffer) - len,
          "Worker %zu (ID: %zu): Steps: %zu, Finished: %zu, State: %s\n",
          i,
          kthread_ids[i],
          worker->statistics.steps,
          worker->statistics.finished,
          print_state(worker->running_task->state)
      );
    } else {
      len += snprintf(
          buffer + len,
          sizeof(buffer) - len,
          "Worker %zu (ID: %zu): Steps: %zu, Finished: %zu, State: IDLE\n",
          i,
          kthread_ids[i],
          worker->statistics.steps,
          worker->statistics.finished
      );
    }
  }

  // Add priority queue statistics
  len += snprintf(
      buffer + len,
      sizeof(buffer) - len,
      "Priority queue sizes: High: %d, Normal: %d, Low: %d\n",
      LIST_EMPTY(&ready_queues[PRIORITY_HIGH]) ? 0 : 1,
      LIST_EMPTY(&ready_queues[PRIORITY_NORMAL]) ? 0 : 1,
      LIST_EMPTY(&ready_queues[PRIORITY_LOW]) ? 0 : 1
  );

  // Write to file using non-blocking I/O
  write(stats_fd, buffer, len);
}

void sched_destroy() {
  for (size_t i = 0; i < SCHED_THREADS_LIMIT; ++i) {
    struct task* task = &tasks[i];
    spinlock_lock(&task->lock);
    if (task->thread != NULL) {
      uthread_free(task->thread);
    }
    spinlock_unlock(&task->lock);
  }

  if (epoll_fd != -1) {
    close(epoll_fd);
    epoll_fd = -1;
  }

  if (stats_fd != -1) {
    close(stats_fd);
    stats_fd = -1;
  }
}

void sched_block(struct task* task) {
  update_task_time(task);

  spinlock_lock(&blocked_lock);
  task->state = UTHREAD_BLOCKED;
  LIST_INSERT_HEAD(&blocked_tasks, task, entries);
  spinlock_unlock(&blocked_lock);

  task_yield(task);
}

/**
 * Блокирует задачу на ожидание событий на файловом дескрипторе.
 */
void sched_block_on_fd(struct task* task, int fd, uint32_t events) {
  update_task_time(task);

  struct epoll_event ev;
  ev.events = events;
  ev.data.ptr = task;

  if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd, &ev) == -1) {
    perror("epoll_ctl: add");
    return;
  }

  spinlock_lock(&blocked_lock);
  task->state = UTHREAD_BLOCKED;
  LIST_INSERT_HEAD(&blocked_tasks, task, entries);
  spinlock_unlock(&blocked_lock);
  sched_adjust_priority(task);
  task_yield(task);

  epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, NULL);
}

// Add task to the appropriate priority queue
void sched_enqueue_task(struct task* task) {
  enum task_priority priority = task->priority;

  spinlock_lock(&queue_locks[priority]);
  LIST_INSERT_HEAD(&ready_queues[priority], task, entries);
  spinlock_unlock(&queue_locks[priority]);
}

// Remove task from its priority queue
void sched_dequeue_task(struct task* task) {
  enum task_priority priority = task->priority;

  spinlock_lock(&queue_locks[priority]);
  LIST_REMOVE(task, entries);
  spinlock_unlock(&queue_locks[priority]);
}

// Adjust task priority based on behavior
void sched_adjust_priority(struct task* task) {
  // If task has been preempted many times, lower its priority
  if (task->preemption_count > 5) {
    if (task->priority < PRIORITY_LOW) {
      task->priority++;
    }
    task->preemption_count = 0;
  }

  // If task has yielded or blocked voluntarily, increase its priority
  if (task->state == UTHREAD_BLOCKED) {
    if (task->priority > PRIORITY_HIGH) {
      task->priority--;
    }
  }
}
