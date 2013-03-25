<body>
  <div style="background-color: #cccccc; font-size: 1.1em;">
    <div style="position: relative; left: 0px;">
      Firmament coordinator <a href="/resource/?id={{RESOURCE_ID}}"><b>{{RESOURCE_ID}}</b></a>
    </div>
    <div style="position: relative; right: 0px;">
      <a href="/">Home</a> &ndash;
      <a href="/jobs">Jobs</a> &ndash;
      <a href="/resources">Resources</a> &ndash;
      <a href="/refs">References</a> &ndash;
      <a href="/shutdown"><span style="color: red;">Shutdown</span></a>
    </div>
  </div>

{{#ERR}}
  <div style="color: red; border: 2px solid red; margin: 5px; padding: 2px;">
    <b>{{ERR_TITLE}}</b><br />
    {{ERR_TEXT}}
  </div>
{{/ERR}}
