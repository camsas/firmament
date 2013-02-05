{{>HEADER}}

{{>PAGE_HEADER}}

<h1>{{RES_ID}}</h1>

<table border="1">
  <tr>
    <td>ID</td>
    <td>{{RES_ID}}</td>
  </tr>
  <tr>
    <td>Friendly name</td>
    <td>{{RES_FRIENDLY_NAME}}</td>
  </tr>
  <tr>
    <td>Type</td>
    <td>{{RES_TYPE}}</td>
  </tr>
  <tr>
    <td>Status</td>
    <td>{{RES_STATUS}}</td>
  </tr>
  <tr>
    <td>Parent ID</td>
    <td><a href="/resource/?id={{RES_PARENT_ID}}">{{RES_PARENT_ID}}</a></td>
  </tr>
  <tr>
    <td>Children IDs</td>
    <td>
      <ul>
      {{#RES_CHILDREN}}
        <li><a href="/resource/?id={{RES_CHILD_ID}}">{{RES_CHILD_ID}}</a></li>
      {{/RES_CHILDREN}}
      </ul>
    </td>
  </tr>
</table>

{{>PAGE_FOOTER}}
