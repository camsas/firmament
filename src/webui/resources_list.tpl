{{>HEADER}}

{{>PAGE_HEADER}}

<h1>Resources</h1>

<table border="1">
  <tr>
    <th>#</th>
    <th>Resource ID</th>
    <th>Friendly name</th>
    <th>State</th>
    <th>Options</th>
  </tr>
  {{#RES_DATA}}
  <tr>
    <td>{{RES_NUM}}</td>
    <td>{{RES_ID}}</td>
    <td>{{RES_FRIENDLY_NAME}}</td>
    <td>{{RES_STATE}}</td>
    <td>
      <a href="/resource/?id={{RES_ID}}">Status</a>
    </td>
  </tr>
  {{/RES_DATA}}
</table>

{{>PAGE_FOOTER}}
