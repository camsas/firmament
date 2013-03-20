{{>HEADER}}

{{>PAGE_HEADER}}

<h1>Ref {{OBJ_ID}}</h1>

<table border="1">
  {{#REF_DATA}}
  <tr>
    <td>Type</td>
    <td>{{REF_TYPE}}</td>
  </tr>
  <tr>
    <td>Scope</td>
    <td>{{REF_SCOPE}}</td>
  </tr>
  <tr>
    <td>Non-deterministic</td>
    <td>{{REF_NONDET}}</td>
  </tr>
  <tr>
    <td>Size</td>
    <td>{{REF_SIZE}}</td>
  </tr>
  <tr>
    <td>Producing task</td>
    <td><a href="/task/?id={{REF_SIZE}}">{{REF_SIZE}}</a></td>
  </tr>
  {{/REF_DATA}}
</table>

{{>PAGE_FOOTER}}
