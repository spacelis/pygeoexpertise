function SpreadsheetFeed(ss_id, sheet_name, have_header) {
  // A iterator through a spreadsheet given spreadsheet id and sheet id
  // if have_header == true, the header will be used as keys to the values
  // of each row.
  this.dsheet = SpreadsheetApp.openById(ss_id).getSheetByName(sheet_name);
  this.data = this.dsheet.getDataRange().getValues();

  if (have_header) {
    this.header = this.data[0];
    this.row_index = 1;
  }
  else {
    this.header = null;
    this.row_index = 0;
  }
  this.next = function () {
    if (this.row_index >= this.data.length) {
      return null;
    }
    if (this.header === null){
      return this.data[this.row_index++];
    }
    else {
      var dict = {};
      for(var i=0;i<this.header.length;i++) {
        dict[this.header[i]] = this.data[this.row_index][i];
      }
      this.row_index++;
      return dict;
    }
  };
  this.skip = function (n) {
    if (this.row_index + n >= this.data.length) {
      return false;
    }
    else {
      this.row_index += n;
    }
  };
}

// [[field]]For marking a question as a question series
var QS_MARKER = /\[\[([a-zA-Z_][a-zA-Z0-9_]*)\]\]/;
// {{field}} Place holder for values to be inserted
var FIELD_MARKER = /\{\{([a-zA-Z_][a-zA-Z0-9_]*)\}\}/g;
// [[~]] For marking the options to be randomized when transforming
var RDM_MARKER = /\[\[~\]\]/;

function getMatches(string, regex, index) {
    if (!index) { index = 1; } // default to the first capturing group
    var matches = [];
    var match;
    while (match = regex.exec(string)) {
        matches.push(match[index]);
    }
    return matches;
}

function mergeObj(obj1, obj2){
  var obj = {};
  for(var key in obj1){obj[key] = obj1[key];}
  for(key in obj2){obj[key] = obj2[key];}
  return obj;
}

// return a random permutation of a range (similar to randperm in Matlab)
// http://groakat.wordpress.com/2012/02/14/random-permutation-of-integers-in-javascript/
function randperm(maxValue){
    // first generate number sequence
    var permArray = new Array(maxValue);
    for(var i = 0; i < maxValue; i++){
        permArray[i] = i;
    }
    // draw out of the number sequence
    for (i = (maxValue - 1); i >= 0; --i){
        var randPos = Math.floor(i * Math.random());
        var tmpStore = permArray[i];
        permArray[i] = permArray[randPos];
        permArray[randPos] = tmpStore;
    }
    return permArray;
}

function transform(src, data) {
  if (typeof(src) !== 'string') {return src;}
  var markers = getMatches(src, FIELD_MARKER);
  Logger.log(markers);
  for(var i in markers) {
    src = src.replace('{{' + markers[i] + '}}', data[markers[i]]);
  }
  src = src.replace(QS_MARKER, '');
  return src.replace(RDM_MARKER, '');
}

function repeatMask(src) {
  var m = src.match(QS_MARKER);
  if(m) {return src.match(QS_MARKER)[1];}
  else {return;}
}

function transformCommonElements(source, target, data, fields) {
  target.setTitle(transform(source.getTitle(), data));
  target.setHelpText(transform(source.getHelpText(), data));
  try {
    target.setRequired(source.isRequired());
  } catch(e) {}
  try {
    target.showOtherOption(source.hasOtherOption());
  } catch(e) {}
  for(var f in fields) {
    var getMethod = 'get' + f;
    var setMethod = 'set' + f;
    target[setMethod](transform(source[getMethod](), data));
  }
}

function transformCHOICES(src, tgt, data) {
  var rdm = src.getTitle().match(RDM_MARKER);
  transformCommonElements(src, tgt, data, []);
  var choices = [];
  var src_choices = src.getChoices();
  if(rdm){
    var rp = randperm(src_choices.length);
    for(var i=0; i<src_choices.length; i++) {
      var c = src_choices[rp[i]];
      choices.push(tgt.createChoice(transform(c.getValue(), data)));
    }
  }
  else {
    for(var i=0; i<src_choices.length; i++) {
      var c = src_choices[i];
      choices.push(tgt.createChoice(transform(c.getValue(), data)));
    }
  }
  tgt.setChoices(choices);
}

function transformSCALE(src, tgt, data) {
  transformCommonElements(src, tgt, data, []);
  tgt.setBounds(transform(src.getLowerBound(), data), transform(src.getUpperBound(), data));
  tgt.setLabels(transform(src.getLeftLabel(), data), transform(src.getRightLabel(), data));
}

function transformItem(template_item, target_form, data) {
  switch(template_item.getType()) {
    case FormApp.ItemType.CHECKBOX:
      transformCHOICES(template_item.asCheckboxItem(), target_form.addCheckboxItem(), data);
      break;
    case FormApp.ItemType.MULTIPLE_CHOICE:
      transformCHOICES(template_item.asMultipleChoiceItem(), target_form.addMultipleChoiceItem(), data);
      break;
    case FormApp.ItemType.SCALE:
      transformSCALE(template_item.asScaleItem(), target_form.addScaleItem(), data);
      break;
    case FormApp.ItemType.PARAGRAPH_TEXT:
      transformCommonElements(template_item.asParagraphTextItem(), target_form.addParagraphTextItem(), data);
      break;
    case FormApp.ItemType.TEXT:
      transformCommonElements(template_item.asTextItem(), target_form.addTextItem(), data);
      break;
    case FormApp.ItemType.SECTION_HEADER:
      transformCommonElements(template_item.asSectionHeaderItem(), target_form.addSectionHeaderItem(), data);
      break;
    case FormApp.ItemType.PAGE_BREAK:
      transformCommonElements(template_item.asPageBreakItem(), target_form.addPageBreakItem(), data);
      break;
    default:
      Logger.log('Fail translating Q: ' + template_item.getTitle());
  }
}

function transformForm(template, form, data) {
  form.setTitle(transform(template.getTitle(), data));
  form.setDescription(transform(template.getDescription(), data));
  form.setAcceptingResponses(template.isAcceptingResponses());
  form.setAllowResponseEdits(template.canEditResponse());
  form.setConfirmationMessage(transform(template.getConfirmationMessage(), data));
  form.setPublishingSummary(template.isPublishingSummary());
  form.setShowLinkToRespondAgain(template.hasRespondAgainLink());
  var items = template.getItems();
  for(var i=0; i<items.length; i++){
    var f_item = items[i];
    var req = repeatMask(f_item.getTitle());
    if (!req) {
      transformItem(f_item, form, data);
    }
    else {
      var subdata = JSON.parse(data[req]);
      for(var idx in subdata) {
        transformItem(f_item, form, mergeObj(data, subdata[idx]));
      }
    }
  }
}

function batch_transform(template_id, datasheet_id, sheet_name, folder_id, form_name, linksheet_id, linksheet, numtoproc) {
  var ls = SpreadsheetApp.openById(linksheet_id).getSheetByName(linksheet);
  var feed = new SpreadsheetFeed(datasheet_id, sheet_name, true);
  var processed = ls.getLastRow() - 1;
  if(processed > 0){
    feed.skip(processed);
  }
  else {
    ls.appendRow(['twitter_id', 'form_id']);
  }
  var template = FormApp.openById(template_id);
  var folder = DriveApp.getFolderById(folder_id);
  var row;
  while((row = feed.next()) && (numtoproc > 0)){
    var form = FormApp.create(form_name);
    transformForm(template, form, row);
    folder.addFile(DriveApp.getFileById(form.getId()));
    DriveApp.getFolderById('root').removeFile(DriveApp.getFileById(form.getId()));
    ls.appendRow([row.twitter_id, form.getId()]);
    numtoproc--;
  }
}


// ------------------- TESTS --------------------------
function test_spreadsheetfeed() {
  var sheetfeed = new SpreadsheetFeed('0AjC0SzqugxC5dDJoNjdYNnhHRjlfX2RPVnlUTEl2R1E', 0, true);
  Logger.log(sheetfeed.next());
}

function test_transform() {
  Logger.log(transform('this is a {{test}}, {{name}}', {'test': 'apple', 'name': 'spaceli'}));
}

function test_transformform() {
  var template = FormApp.openById('1-9MKCzx3nU9dhfZY70OoC7_Bg7Xr5IiUv8cHTYl0UI8');
  var data = {'twitter_id': 'GeoExpertise',
              'expr': 'spacelis',
              'city': '[{"a": 1, "b": 2}, {"a": 3, "b": 4}, {"a": 5, "b": 6}]',
             };
  var form = FormApp.create('testcreatedform');
  transformForm(template, form, data);
}

function test_re(){
  var re = /\[\[(\d+)\]\]/;
  Logger.log('adfionpadifp [[9]] aisdofnp iop[[3]]'.match(re)[1]);
}

//--------------------- MAIN -----------------------------

function transform_main(){
  batch_transform(
      '1by8Z7uBGH6S0hGv6wvXxUyoyjCiGiUvrh5STS6b2xhM', // template id
      '0AjC0SzqugxC5dDJoNjdYNnhHRjlfX2RPVnlUTEl2R1E', // datasheet id
      'geoexpert.expertise',                          // sheet name
      '0BzC0SzqugxC5emxqRzRMUld6S3c',                 // folder id where to store generated forms
      'Geo-expertise Evaluation',                     // Forms' name
      '0AjC0SzqugxC5dEI4UkFLTkxYeTgyTDFZd2p0OEZ2VFE', // Spreadsheet mapping twitter ids to form ids
      'user2form',                                    // Mapping sheet name
      50                                              // Number to process
      );
}

