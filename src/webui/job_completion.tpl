{{>HEADER}}

{{>PAGE_HEADER}}

<h1>Job {{JOB_ID}}</h1>

<p>This page monitors the completion for job <b>{{JOB_ID}}</b>; it refreshes automatically every 10 seconds.</p>

<script type="text/javascript">
$(function() {
  if ("{{JOB_STATUS}}" == "COMPLETED") {
    alert("Job {{JOB_ID}} has completed!");
  }
});
</script>

<!-- should be in header, but works here too -->
<meta http-equiv="refresh" content="10" />

<table class="table table-bordered">
  <tr>
    <td>ID</td>
    <td><a href="/job/status/?id={{JOB_ID}}">{{JOB_ID}}</a></td>
  </tr>
  <tr>
    <td>Name</td>
    <td>{{JOB_NAME}}</td>
  </tr>
  <tr>
    <td>Status</td>
    <td>{{JOB_STATUS}}</td>
  </tr>
</table>

{{>PAGE_FOOTER}}
