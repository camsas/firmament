{
  "id": "{{TASK_ID}}",
  "name": "{{TASK_NAME}}",
  "job_id": "{{TASK_JOB_ID}}",
  "binary": "{{TASK_BINARY}}",
  "args": "{{TASK_ARGS}}",
  "tecs": [
    {{#TASK_TECS}}
      "{{TASK_TEC}}"{{#TASK_TECS_separator}}, {{/TASK_TECS_separator}}
    {{/TASK_TECS}}
  ]
  "status": "{{TASK_STATUS}}",
  "timestamps": {
    "submit": "{{TASK_SUBMIT_TIME}}",
    "scheduled": "{{TASK_START_TIME}}",
    "finished": "{{TASK_FINISH_TIME}}"
  },
  "last_heartbeat": "{{TASK_LAST_HEARTBEAT}}",
  "resource": "{{TASK_SCHEDULED_TO}}",
  "dependencies": [
    {{#TASK_DEPS}}
      "{{TASK_DEP_ID}}"{{#TASK_DEPS_separator}}, {{/TASK_DEPS_separator}}
    {{/TASK_DEPS}}
  ],
  "spawned": [
    {{#TASK_SPAWNED}}
      "{{TASK_SPAWNED_ID}}"{{#TASK_SPAWNED_separator}}, {{/TASK_SPAWNED_separator}}
    {{/TASK_SPAWNED}}
  ],
  "outupts": [
    {{#TASK_OUTPUTS}}
      "{{TASK_OUTPUT_ID}}"{{#TASK_OUTPUTS_separator}}, {{/TASK_OUTPUTS_separator}}
    {{/TASK_OUTPUTS}}
  ]
}
