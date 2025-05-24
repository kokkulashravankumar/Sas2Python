  <body onload="makeGetRequest()">
  <div id="file-content">
        <textarea id="output-content"></textarea>
  </div>
  <script>
  let output;
  function makeGetRequest() {
    fetch('/excutecode')
    .then((response) => {
        if (response.ok) {
            return response.blob();
        } else {
            throw new Error('Error fetching file.');
        }
    })
    .then((data) => {
        let file = new File([data], "file.txt", {type: "text/plain"});
        let reader = new FileReader();
        reader.onload = function() {
            let fileContent = reader.result;
            document.getElementById("output-content").value = fileContent;
        }
        reader.readAsText(file);
    })
    .catch((error) => console.error(error));
  }
  </script>
  </body>
