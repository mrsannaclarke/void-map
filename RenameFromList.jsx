// Rename selected Illustrator objects from a text file
#target illustrator

var f = File.openDialog("Select your exported layer name list (.txt)");
if (!f) { alert("No file selected."); exit(); }

f.open("r");
var names = [];
while (!f.eof) {
    var line = f.readln();
    if (line) {
        // remove any carriage returns or spaces manually
        line = line.replace(/^\s+|\s+$/g, "");
        if (line.length > 0) names.push(line);
    }
}
f.close();
names.reverse(); // flip Photoshop order to match Illustrator selection order

var sel = app.activeDocument.selection;
if (sel.length !== names.length) {
    alert("⚠️ Selected object count (" + sel.length + ") ≠ name count (" + names.length + ").");
} else {
    for (var i = 0; i < sel.length; i++) {
        sel[i].name = names[i];
    }
    alert("✅ Renamed " + sel.length + " objects from list.");
}