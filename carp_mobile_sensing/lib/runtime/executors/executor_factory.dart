/*
 * Copyright 2018-2022 Copenhagen Center for Health Technology (CACHET) at the
 * Technical University of Denmark (DTU).
 * Use of this source code is governed by a MIT-style license that can be
 * found in the LICENSE file.
 */

part of runtime;

class ExecutorFactory {
  static final ExecutorFactory _instance = ExecutorFactory._();
  ExecutorFactory._();
  factory ExecutorFactory() => _instance;

  final Map<int, TriggerExecutor> _triggerExecutors = {};

  /// Returns the relevant [TaskControlExecutor] based on the type of [trigger]
  /// and [task].
  TaskControlExecutor getTaskControlExecutor(
    TaskControl taskControl,
    TriggerConfiguration trigger,
    TaskConfiguration task,
  ) {
    // a TriggeredAppTaskExecutor need BOTH a [Schedulable] trigger and an [AppTask]
    // to schedule
    if (trigger is Schedulable && task is AppTask) {
      return AppTaskControlExecutor(taskControl, trigger, task);
    }

    // all other cases we use the normal background triggering relying on the app
    // running in the background
    return TaskControlExecutor(taskControl, trigger, task);
  }

  /// Get the [TriggerExecutor] for a [triggerId], if available.
  TriggerExecutor? getTriggerExecutor(int triggerId) =>
      _triggerExecutors[triggerId];

  /// Create a [TriggerExecutor] based on the [trigger] type.
  TriggerExecutor createTriggerExecutor(
    int triggerId,
    TriggerConfiguration trigger,
  ) {
    if (_triggerExecutors[triggerId] == null) {
      TriggerExecutor executor = ImmediateTriggerExecutor();

      switch (trigger.runtimeType) {
        case const (ElapsedTimeTrigger):
          executor = ElapsedTimeTriggerExecutor();
          break;
        case const (ScheduledTrigger):
          warning("ScheduledTrigger is not implemented yet. "
              "Using an 'ImmediateTriggerExecutor' instead.");
          executor = ImmediateTriggerExecutor();
          break;
        case const (NoOpTrigger):
          executor = NoOpTriggerExecutor();
          break;
        case const (ImmediateTrigger):
          executor = ImmediateTriggerExecutor();
          break;
        case const (OneTimeTrigger):
          executor = OneTimeTriggerExecutor();
          break;
        case const (DelayedTrigger):
          executor = DelayedTriggerExecutor();
          break;
        case const (PeriodicTrigger):
          executor = PeriodicTriggerExecutor();
          break;
        case const (DateTimeTrigger):
          executor = DateTimeTriggerExecutor();
          break;
        case const (RecurrentScheduledTrigger):
          executor = RecurrentScheduledTriggerExecutor();
          break;
        case const (CronScheduledTrigger):
          executor = CronScheduledTriggerExecutor();
          break;
        case const (SamplingEventTrigger):
          executor = SamplingEventTriggerExecutor();
          break;
        case const (ConditionalSamplingEventTrigger):
          executor = ConditionalSamplingEventTriggerExecutor();
          break;
        case const (ConditionalPeriodicTrigger):
          executor = ConditionalPeriodicTriggerExecutor();
          break;
        case const (RandomRecurrentTrigger):
          executor = RandomRecurrentTriggerExecutor();
          break;
        case const (PassiveTrigger):
          executor = PassiveTriggerExecutor();
          break;
        case const (UserTaskTrigger):
          executor = UserTaskTriggerExecutor();
          break;
        default:
          warning(
              "Unknown trigger used - cannot find a TriggerExecutor for the trigger of type '${trigger.runtimeType}'. "
              "Using an 'ImmediateTriggerExecutor' instead.");
          executor = ImmediateTriggerExecutor();
      }
      _triggerExecutors[triggerId] = executor;
    }
    return _triggerExecutors[triggerId]!;
  }

  /// Create a [TaskExecutor] for a [task] based on the task type.
  TaskExecutor createTaskExecutor(TaskConfiguration task) {
    if (task is AppTask) return AppTaskExecutor();
    if (task is FunctionTask) return FunctionTaskExecutor();
    return BackgroundTaskExecutor();
  }
}
