var dstk = require("dstk");
require('console-stamp')(console, '[HH:MM:ss.l]');
 

var mysql = require("mysql");

var connection = mysql.createConnection({
  host: "localhost",
  port: 3306,

  // Your username
  user: "root",

  // Your password
  password: "",

  // Your database name
  database: "addressesToGeoDB"
});

connection.connect(function(err) {
  if (err) throw err;
  console.log("connected as id " + connection.threadId + "\n");
  geocodeAddresses();
  
});


//Gets and concatenates address fields from Addresses table and passes them through Geocoder to get geocoordinates
function geocodeAddresses() {
	//grab voters from Addresses table, update table name to match your dataset
	//rename any columns to match your dataset as needed

	var query = connection.query(
		"SELECT id, streetNum, zip, streetName, city, state FROM Addresses WHERE longitude IS NULL LIMIT 25000",  
		function(err, res){

			var counter = 0;

			//go through each address record and grab address fields
			res.forEach(function(addresses, index, result){
				var address;
				var number = addresses.streetNum;
				var zip = addresses.zip;
				var street = addresses.streetName;
				var city = adddresses.city;
				var state = addresses.state;

				//concatenate address fields
				address = number +" "+ street +" "+ city +" "+ state +" "+ zip;
				console.log(address);

				//call dstk street2coordinates function to grab geocoordinates, call addLongLat to set longLat column values
				dstk.street2coordinates(address, function(err, data){

					counter ++;
					if (err) {
						// console.log(err);
						var longitude = "error";
						var latitude = "error";
						var id = addresses.id;
						console.log(`ID: ${id}\nLongitude: ${longitude}\nLatitude: ${latitude}\nCounter: ${counter}`);
					}
					else if (!data[address]) {
						var longitude = "error";
						var latitude = "error";
						var id = addresses.id;
						console.log(`ID: ${id}\nLongitude: ${longitude}\nLatitude: ${latitude}\nCounter: ${counter}`);
					}

					else {
						console.log(`${data[address].longitude}, ${data[address].latitude}`);

						var longitude = data[address].longitude;
						var latitude = data[address].latitude;
						var id = addresses.id;
						addLongLat(longitude, latitude, voterId);
						console.log(`ID: ${id}\nLongitude: ${longitude}\nLatitude: ${latitude}\nCounter: ${counter}`);
					}
				});
				
			});
			
	});

	//recursion to re-trigger the script every 90 seconds so your entire dataset is effectively looped through 25,000 entries at a time and the stack is not overloaded
  setTimeout(geocodeAddresses, 90000);

};

//Adds geocoordinates to longLat column in Addresses
function addLongLat(longitude, latitude, voterId){
	var query = connection.query(
		"UPDATE Addresses SET longitude = ?, lat = ? WHERE id = ?", [longitude, latitude, id], 
		function(err, res){
			if (err){
				console.log(err);
			}
		});
};