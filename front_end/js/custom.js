$(document).ready(function () {

});

class CosSimilarityMap {
  constructor(compareddevice, cosinesim, featurevector) {
    this.compareddevice = compareddevice;
    this.cosinesim = cosinesim;
    this.featurevector = featurevector;
  }
}

deviceDropdownValChanged = function()
{
  var currDevSel = $('#sel1').val();
  if(currDevSel  != "Device ID!")
  {
  $body = $("body");
  $body.addClass("loading");


  $.ajax({
    type: "GET",
    url: "motion_split_files_10_mins_window/inserted_missing_values/" + currDevSel + "-motion-Normalized.csv",
    dataType: "text",
    success: function(data) {
      processData(data, 0, null);
      getSimilaryPlots(currDevSel);
    },
    error: function(xhr, ajaxOptions, thrownError) {
      alert("Status: " + xhr.status + "     Error: " + thrownError);
    }
  });
}
    //$body.removeClass("loading");
  }

plotCurrentObject = function(currObj, index)
{
  var a = [];
   $.ajax({
    type: "GET",
    url: "motion_split_files_10_mins_window/inserted_missing_values/" + currObj.compareddevice + "-motion-Normalized.csv",
    dataType: "text",
    success: function(data) {
      processData(data, index, currObj); 

    },
    error: function(xhr, ajaxOptions, thrownError) {
      alert("Status: " + xhr.status + "     Error: " + thrownError);
    }
  });
}
  getSimilaryPlots = function(currDevSel)
  {
    var listObj = [];
    $.ajax({
      type: "GET",
      url: "motion_split_files_10_mins_window/CosineSimilarity_Normalized/" + currDevSel + "-motion-Normalized-CosineOccupancy.csv",
      dataType: "text",
      success: function(data) {
        var allTextLines = data.split(/\r\n|\n/);
        for (var i = 2; i < 12; i++) {
          var dataLine = allTextLines[i].split(',');
                //var tarr = [];
                if (dataLine.length == 5) {
                 listObj.push(new CosSimilarityMap(parseInt(dataLine[1]),parseFloat(dataLine[2]), parseInt(dataLine[3])));
                   //tarr.push();          
                  //lines.push(Number(data[1]), parseFloat(data[2]));                      
                }
                //lines.push(tarr);
              }
             for(var i = 1; i <= listObj.length; i++)
          {
            var index = i - 1;
            var currObj = listObj[index];
            plotCurrentObject(currObj, i);
          }
       
            },
            error: function(xhr, ajaxOptions, thrownError) {
              alert("Status: " + xhr.status + "     Error: " + thrownError);
            }
          });
     
  }

  processData = function(data, index, currObj)
  {
   var allTextLines = data.split(/\r\n|\n/);
   var lines = [];

   for (var i = 1; i < allTextLines.length; i++) {
    var data = allTextLines[i].split(',');
        //var tarr = [];
        if (data.length == 3) {
         lines.push([new Date(Number(data[1])), parseFloat(data[2])]);
           //tarr.push();          
          //lines.push(Number(data[1]), parseFloat(data[2]));                      
        }
        //lines.push(tarr);
      }
      var divID = "adiv" + index; 
      if(index == 0)
      {
        divID = "adiv";
      }
$('#'+divID).show();

      g = new Dygraph(
       document.getElementById(divID),
       lines,
        //"6-motion-Normalized_V2.csv",
        {
         labels: [ "timstamp", "motion" ],
         rightGap: '5',
         animatedZooms: true,
         axes: {
          x: {
            drawGrid: false
          },
          y: {
            gridLineColor: 'blue'
          }
        }
      }
      );
 

 if(index != 0)
{

  $('#'+divID).prev('.eachInfoDiv').show();
  $('#'+divID).prev('.eachInfoDiv').find('.infoDivText').text('DEVICE - ' + currObj.compareddevice + ' | COSINE SIMILARITY - ' + currObj.cosinesim + ' | FEATURE VECTOR LENGTH - ' + currObj.featurevector);
  // $('#'+divID).attr("data-device", currObj.compareddevice);
  // $('#'+divID).attr("data-cosinesim", currObj.cosinesim);
  // $('#'+divID).attr("data-featurevector", currObj.featurevector);
  
}     
if(index == 9)
{
     $body = $("body");
  $body.removeClass("loading");
}
    }

