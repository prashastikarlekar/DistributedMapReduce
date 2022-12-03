/** @format */

function readFile() {
	inp = document.getElementById("inputBox").value.trim();

	fetch("response.json")
		.then((response) => response.text())
		.then((text) => parseFile(text, inp));
	// outputs the content of the text file
}

function parseFile(text, input) {
	resultDiv = document.getElementById("result");

	// console.log(text)
	var regex =
		/\[\\"[A-Za-z0-9]+\\",\s+\\"[A-Za-z0-9]+\.[A-Za-z0-9]+\\"]":\s+\[\s+[0-9]+\s+]/g;
	var result = text.match(regex);
	console.log(result.length);
	var obj = {};

	i = 0;
	searches = 0;
	while (i < result.length) {
		current = result[i];
		firstPart = current.split(":")[0];
		secondPart = current.split(":")[1];

		key = firstPart.split(",")[0];
		key = key.substring(3, key.length - 2);

		fileName = firstPart.split(",")[1];
		fileName = fileName.substring(3, fileName.length - 4);

		count = JSON.parse(secondPart)[0];

		if (key == input) {
			searches += 1;

			match =
				"Word: <b class='text-info'>" +
				input +
				"</b> appeared " +
				count +
				" times in file " +
				fileName;
			obj[key] = "<div><h5>" + match + "</h5><div>";
			// msg =
			// 	"Word: " + input + " appeared " + count + " times in file " + fileName;
			// alert(
			// 	"Word: " + input + " appeared " + count + " times in file " + fileName
			// );
			resultDiv.innerHTML += obj[key];

			// console.log(resultDiv.innerText);
		}

		i += 1;
	}
	if (searches == 0) {
		alert("Word: " + input + " did not appear in the files. Try another word.");
	}
}
