<body role="document">
  <nav class="navbar navbar-inverse navbar-fixed-top">
    <div class="container">
      <div class="navbar-header">
        <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
          <span class="sr-only">Toggle navigation</span>
          <span class="icon-bar"></span>
          <span class="icon-bar"></span>
          <span class="icon-bar"></span>
        </button>
        <a class="navbar-brand" href="/resource/?id={{RESOURCE_ID}}">{{RESOURCE_ID}}</a>
      </div>
      <div id="navbar" class="navbar-collapse collapse">
        <ul class="nav navbar-nav">
          <li class="active"><a href="/">Home</a></li>
          <li><a href="/jobs">Jobs</a></li>
          <li><a href="/resources">Resources</a></li>
          <li><a href="/refs">References</a></li>
          <li class="dropdown">
            <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-expanded="false">Actions <span class="caret"></span></a>
            <ul class="dropdown-menu" role="menu">
              <li><a href="/shutdown">Shutdown</a></li>
            </ul>
          </li>
        </ul>
      </div><!--/.nav-collapse -->
    </div>
  </nav>

<div class="container" role="main">

{{#ERR}}
  <div class="alert alert-danger" role="alert">
    <strong>{{ERR_TITLE}}</strong><br />
    {{ERR_TEXT}}
  </div>
{{/ERR}}
