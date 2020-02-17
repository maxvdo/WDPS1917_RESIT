import sys,os

gold_file = sys.argv[1]
pred_file = sys.argv[2]

# Load the gold standard
gold = {}
for line in open(gold_file):
    record, string, entity = line.strip().split('\t', 2)
    gold[(record, string)] = entity
n_gold = len(gold)
print('Gold entities: %s' % n_gold)

# Load the predictions
pred = {}
for line in open(pred_file,encoding="utf-8"):
    record, string, entity = line.strip().split('\t', 2)
    pred[(record, string)] = entity
n_predicted = len(pred)
print('Linked entities: %s' % n_predicted)

# Evaluate predictions
n_correct = sum( int(pred[i]==gold[i]) for i in set(gold) & set(pred) )

print("Amount of mappings: %s" % len(set(gold) & set(pred)))
print('Correct mappings: %s' % n_correct)
print("Amount of incorrect: mappings: %s" % (len(set(gold) & set(pred)) - n_correct))

'''
if not os.path.exists(os.path.dirname("./incorrect_samples/")):
    os.makedirs(os.path.dirname("./incorrect_samples/"))

file = open("./incorrect_samples/incorrect_mapping.txt","w+",encoding="utf-8")
line = "(Doc,Word)" + '\t\t\t\t\t' + "Predictions" + '\t\t' + "Gold" + "\n"
file.write(line)

for i in (set(gold) & set(pred)):
    if not int(pred[i]==gold[i]):
        line = str(i) + '\t' + pred[i] + '\t' + gold[i] + "\n"
        file.write(line)

file = open("./incorrect_samples/entities_not_recognized.txt","w+",encoding="utf-8")
line = "(Doc,Word)" +  "\t\t" + "Gold label" + "\n"
file.write(line)
for i in (set(gold) - (set(gold) & set(pred))):
    line = str(i) + '\t' +  gold[i] + "\n" #+ '\t' + pred[i] + '\t' + gold[i] + "\n"
    file.write(line)
'''

# Calculate scores
precision = float(n_correct) / float(n_predicted)
print('Precision: %s' % precision )
recall = float(n_correct) / float(n_gold)
print('Recall: %s' % recall )
f1 = 2 * ( (precision * recall) / (precision + recall) )
print('F1: %s' % f1 )
