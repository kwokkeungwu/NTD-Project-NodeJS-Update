var output = [];
var output_details = [];
var output_other = [];
var output_other_details = [];
var output_human_date = [];
var output_full_details = [];
var output_max_value = [];
var output_max_values = [];
var count = 0
var count_other = 0;

var csv = require('csv');
var obj = csv();
var objDetails = csv();
var objOther = csv();
var objOtherDetails = csv();
var objDailyTopPosts = csv();
var fs = require('fs');
var unique = require('array-unique');
var datetime = require('node-datetime');

var lineReader = require('readline').createInterface({	
  input: require('fs').createReadStream('./posts.csv', 'utf8')
});

lineReader.on('line', function (line) {
    var jsonFromLine = {};
	var jsonFromLineDetails = {};
	var jsonFromLineDate = {};
    var lineSplit = line.split(',');
    jsonFromLineDetails.id = lineSplit[0];
    jsonFromLineDetails.title = lineSplit[1];
	jsonFromLineDetails.privacy = lineSplit[2];
	jsonFromLineDetails.likes = lineSplit[3];
	jsonFromLineDetails.views = lineSplit[4];
	jsonFromLineDetails.comments = lineSplit[5];
	jsonFromLineDetails.timestamp = lineSplit[6];	
	jsonFromLine.id = lineSplit[0];
	jsonFromLineDate = lineSplit[6];
	
    if (jsonFromLineDetails.privacy === 'public' && 
	    (jsonFromLineDetails.comments > 10 && jsonFromLineDetails.views > 9000) &&
		jsonFromLineDetails.title.length < 40 ) 
		{
			output.push(jsonFromLine);	
			output_details.push(jsonFromLineDetails);
			count++;
		}
		
	if (jsonFromLineDetails.privacy !== 'public' || 
	    (jsonFromLineDetails.comments <= 10 || jsonFromLineDetails.views <= 9000) ||
		jsonFromLineDetails.title.length >= 40 ) {
			output_other.push(jsonFromLine);
			output_other_details.push(jsonFromLineDetails);
			count_other++
		}
		
		output_full_details.push(jsonFromLineDetails);
		var human_date = String(datetime.create(lineSplit[6]).format('Y-m-d'));		
		if (human_date.search('2015') == 0) {
			output_human_date.push(human_date);
		}		
		unique(output_human_date);
}); 

lineReader.on('close', function (line) {
    console.log(output); // list output	
	obj.from.array(output).to.path('./results/top_posts.csv'); 
	
	fs.writeFile("./results/top_posts.json", JSON.stringify(output), function(err) {
    if (err) throw err;
    console.log('complete');
    });
	
	console.log(output_details); // list output details
	objDetails.from.array(output_details).to.path('./results/top_posts_details.csv');
	
	fs.writeFile("./results/top_posts_details.json", JSON.stringify(output_details), function(err) {
    if (err) throw err;
    console.log('complete');
    });
	
	console.log(output_other); // list other output
	objOther.from.array(output_other).to.path('./results/other_posts.csv');
	
	fs.writeFile("./results/other_posts.json", JSON.stringify(output_other), function(err) {
    if (err) throw err;
    console.log('complete');
    });
	
	console.log(output_other_details); // list other output details
	objOtherDetails.from.array(output_other_details).to.path('./results/other_posts_details.csv');	
	
	fs.writeFile("./results/other_posts_details.json", JSON.stringify(output_other_details), function(err) {
    if (err) throw err;
    console.log('complete');
    });
	
	console.log("count: " + count); // count 
	console.log("other count: " + count_other); // count_other 

	var output_full_details_daily_tops = [];
	
	var max = -Infinity;
	var day_index = -Infinity;
	
	
	for(var index = 0; output_human_date.length > index; index++){	
		day_index = String(output_human_date[index]);			
		for(var pos = 0; output_full_details.length > pos; pos++){											
			var position_date = String(datetime.create(output_full_details[pos].timestamp).format('Y-m-d'));								
			if (day_index == position_date){					
				if (parseInt(output_full_details[pos].likes) > max){
					max = output_full_details[pos].likes;						
				}					
			}              			
		}
		output_max_values.push(max);			
		max = -Infinity;
	}
		
	for(var index = 0; output_human_date.length > index; index++){
        var fromLine = {};	
		fromLine.likes = output_max_values[index];		
		fromLine.timestamp = String(output_human_date[index]);	
		output_full_details_daily_tops.push(fromLine);
	}
		
	console.log(output_full_details_daily_tops);
					
	objDailyTopPosts.from.array(output_full_details_daily_tops).to.path('./results/daily_top_posts.csv');	
	
	fs.writeFile("./results/daily_top_posts.json", JSON.stringify(output_full_details_daily_tops), function(err) {
    if (err) throw err;
    console.log('complete');
    });
		
});

