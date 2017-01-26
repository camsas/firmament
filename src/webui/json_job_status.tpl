{
  "id": "{{JOB_ID}}",
  "name": "{{JOB_NAME}}",
  "status": "{{JOB_STATUS}}",
  "root_task": "{{JOB_ROOT_TASK_ID}}",
  "outputs": [
    {{#JOB_OUTPUTS}}
      "{{JOB_OUTPUT_ID}}"{{#JOB_OUTPUTS_separator}}, {{/JOB_OUTPUTS_separator}}
    {{/JOB_OUTPUTS}}
  ]
}
