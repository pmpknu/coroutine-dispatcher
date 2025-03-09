# Отчет по лабораторной работе №4: Диспетчер корутин

## Титульный лист
###### _Федеральное государственное автономное образовательное учреждение высшего образования «Национальный исследовательский университет ИТМО» Факультет программной инженерии и компьютерной техники Направление подготовки 09.03.04 «Программная инженерия» – Системное и прикладное программное обеспечение_

### ЛР4: Диспетчер корутин

- __Выполнил:__ [Горляков Даниил Петрович](https://github.com/pmpknu)
- __Группа:__ P3315
- __Принял:__ [Романов Артемий Ильич](https://github.com/artemiyjjj)

###### Санкт-Петербург, 2025

## Введение

В рамках данной лабораторной работы был разработан диспетчер корутин (файберов) с поддержкой многопоточности и событийно-ориентированного планирования. Диспетчер обеспечивает эффективное управление задачами, оптимизируя использование процессорного времени и обработку ввода-вывода.

## Реализованные компоненты

### 1. Базовая архитектура планировщика

Дополнена базовая архитектура планировщика:
- Создания и управления корутинами

```c
struct task {
  struct uthread* thread;
  struct worker* worker;
  enum {
    UTHREAD_RUNNABLE,
    UTHREAD_RUNNING,
    UTHREAD_FINISHED,
    UTHREAD_ZOMBIE,
    UTHREAD_BLOCKED, // Состояние блокировки
  } state;
  
  struct spinlock lock;
  struct timespec last_state_change; // Время последнего изменения состояния
  uint64_t time_running; // Всего времени в состоянии RUNNING
  uint64_t time_runnable; // Всего времени в состоянии RUNNABLE
  uint64_t time_blocked; // Всего времени в состоянии BLOCKED
  
  LIST_ENTRY(task) entries; // Узел двусвязного списка задач
  enum task_priority priority; // Приоритет задачи
  uint64_t quantum_start; // Время, когда задача начала свою текущую кванту
  uint32_t preemption_count; // Количество раз, когда задача была прервана
};
```

### 2. Event-driven планировщик

- Добавлен epoll
- Блокировка задач на ожидание событий I/O
- Добавлен отдельный поток для обработки событий epoll

```c
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
```

### 3. Приоритетное планирование

Внедрена система приоритетов для задач:
- Задачи распределяются по трем уровням приоритета (высокий, нормальный, низкий)
- Приоритет динамически корректируется в зависимости от поведения задачи
- Задачи с интенсивным вводом-выводом получают более высокий приоритет

```c
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
```

### 4. Сбор и вывод статистики

- Отслеживание времени выполнения задач в различных состояниях
- Подсчет количества переключений контекста и завершенных задач
- Неблокирующий вывод статистики в файл с использованием асинхронного ввода-вывода

```c
void sched_print_statistics() {
  if (stats_fd == -1) return;
  
  uint64_t now = get_time_ns();
  
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

  // ... сбор статистики ...

  // Format statistics into a buffer
  char buffer[4096];
  int len = snprintf(/* ... */);

  // Write to file using non-blocking I/O
  write(stats_fd, buffer, len);
}
```

## Результаты тестирования

Планировщик прошел тестирование в различных конфигурациях:
- Компиляция с разными компиляторами (gcc, clang)
- Различные уровни оптимизации (-O0, -O3)
- Тестирование с санитайзерами (address, leak)

## Вывод

В результате выполнения лабораторной работы был создан диспетчер корутин с поддержкой event-driven (feedback) планирования.
Изначально было непонятно, что следовало сделать, на мой взгляд, условия задачи были некорректно поставлены.
В итоге я смог выделить следующее:
- Добавил состояние UTHREAD_BLOCKED
- Добавил неблокирующий I/O (epoll)
- Изменил планировщик на feedback
- Добавил сбор и неблокирующий вывод статистики в файл

