// Export Layer Names to Text File
#target photoshop

// Pick a save location
var doc = app.activeDocument;
var file = File.saveDialog("Save layer name list as", "Text File:*.txt");
if (!file) { alert("Cancelled."); exit(); }

file.open("w");

function listLayers(layerSet, prefix) {
    for (var i = 0; i < layerSet.layers.length; i++) {
        var layer = layerSet.layers[i];
        if (layer.typename === "ArtLayer") {
            file.writeln(prefix + layer.name);
        } else if (layer.typename === "LayerSet") {
            listLayers(layer, prefix + layer.name + "/");
        }
    }
}

listLayers(doc, "");
file.close();
alert("Done! Layer names saved to:\n" + file.fsName);