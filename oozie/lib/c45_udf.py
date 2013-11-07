import sys
sys.path += ["/usr/lib/jython/Lib","/usr/lib/jython/Lib/site-packages","/usr/lib/jython/Lib/site-packages/simplejson-2.1.0-py2.5.egg"]
import java.io.ByteArrayOutputStream
import java.io.ObjectOutputStream
import weka.core.Instances as Instances
import weka.core.Instance as Instance
import weka.core.FastVector as FastVector
import weka.core.Attribute as Attribute
import weka.classifiers.trees.J48 as J48
import simplejson

def parse_tree(a):
    tree = a[3:].split("\n")
    tree_hash = {}
    for line in tree:
        if line:
            if line[0] != " " or line[0] != "\n" and "Number of Leaves" not in line and "Size of the tree" not in line:
                entry = map(lambda x:x.strip(), line.split("|"))
                count_until = 0
                for i in entry:
                    if i == "":
                        count_until += 1
                    else:
                        break
                add_to_tree_hash(count_until, entry[count_until], tree_hash)

    return simplejson.dumps(tree_hash)
    
def add_to_tree_hash(level, key, hash):
    if level not in hash:
        hash[level] = []
    hash[level].append(key)
    return hash

def output_serialized_model(key, model):
    baos = ByteArrayOutputStream()
    oos = ObjectOutputStream(baos)
    oos.writeObject(model)
    oos.flush()
    oos.close()
    return baos.toByteArray()

@outputSchema("state:chararray, model:chararray")
def build_instances(state,dataset):
    class_attributes = ["Sunny", "Fog", "Rain", "Snow", "Hail", "Thunder", "Tornado"]
    header = ["state","lat", "lon", "day","temp","dewp","weather"]

    #build attributes based on the header and types
    attributes = []
    for h in header[:-1]:
        attributes.append(Attribute(h))

    #add the classification attribute
    classification_vector = FastVector(len(class_attributes))
    for c in class_attributes:
        classification_vector.addElement(c)
    attributes.append(Attribute("toClassify", classification_vector))

    fvWekaAttributes = FastVector(len(dataset[0]))

    for a in attributes:
        fvWekaAttributes.addElement(a)
    
    training_set = Instances("C4.5Set", fvWekaAttributes, len(dataset))
    training_set.setClassIndex(len(header)-1)

    for d in dataset:
        inst = Instance(len(d))
        for i in range(len(d)-1):
            try:
                inst.setValue(fvWekaAttributes.elementAt(i), float(d[i]))
            except:
                pass
                #print "failed on", i, d[i], d[i].__class__
        inst.setValue(fvWekaAttributes.elementAt(len(d)-1), d[-1])
        
        training_set.add(inst)


    j48 = J48()
    j48.buildClassifier(training_set)
    return state,parse_tree(str(j48))

