from transformers import TrainingArguments, AutoModelForSequenceClassification, Trainer, AutoTokenizer, AutoConfig, AutoModel, PreTrainedModel
import polars as pl
from datasets import Dataset
import numpy as np
from sklearn.metrics import precision_recall_fscore_support
MODELS = {
        'Bio_ClinicalBERT' : 'emilyalsentzer/Bio_ClinicalBERT', ## used by BERTMAP, may be better for clinical use cases.
        'PubMedBERT': 'microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext', ## PubMedBERT, uses PubMed so may be good for research terms
        'SapBERT': 'cambridgeltl/SapBERT-from-PubMedBERT-fulltext', ## SapBert trained with UMLS as KG
        }
model_name = 'SapBERT'
model_path = MODELS[model_name]



model = AutoModelForSequenceClassification.from_pretrained(model_path, num_labels=3)
tokenizer = AutoTokenizer.from_pretrained(model_path)
## load dataset
data_path = 'generated_maps.parquet'
df = pl.read_parquet(data_path)
lines = []
for row in df.iter_rows(named = True):
    line = {}
    line['txt'] = f"{row['source prefix']} | {row['source name']} | {', '.join(row['source descendant names'])} | {', '.join(row['source ancestor names'])} [SEP] {row['target prefix']} | {row['target name']} | {', '.join(row['target descendant names'])} | {', '.join(row['target ancestor names'])}"
    line['class']= row['class']
    lines.append(line)
dataset = Dataset.from_list(lines)
tokenizer = AutoTokenizer.from_pretrained(model_path)
def tokenize(row):
    rep = tokenizer(
        row['txt'],
        padding='max_length',
        truncation=True,
        max_length=256,  # adjust as needed
        return_tensors='pt'
    )
    rep['label'] = row['class']
    return rep
dataset = dataset.map(tokenize, batched=True)
split_dataset = dataset.train_test_split(test_size=0.3, seed=101, shuffle=True)
train_dataset = split_dataset["train"]
temp_dataset = split_dataset["test"]
val_test_split = temp_dataset.train_test_split(test_size=0.3, seed=101, shuffle=True)
val_dataset = val_test_split["train"]
test_dataset = val_test_split["test"]


## define loss 
def compute_metrics(p):
    preds = np.argmax(p.predictions, axis=1)
    labels = p.label_ids
    precision, recall, f1, _ = precision_recall_fscore_support(
        labels, preds, average="macro", zero_division=0
    )
    return {"precision": precision, "recall": recall, "f1": f1}


### define trainer
training_args = TrainingArguments(
        eval_strategy="epoch",
        save_strategy="epoch",
        logging_strategy="epoch",
        learning_rate=2e-5,
        per_device_train_batch_size=16,
        per_device_eval_batch_size=16,
        num_train_epochs=10,
        weight_decay=0.01,
        save_total_limit=1,
        logging_dir="./logs",
    )


trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    compute_metrics=compute_metrics,
)
## train model 
# trainer.train()

# # ## save res 
# model.save_pretrained("./output/refinenet", from_pt=True)

## load the saved model and run on testset
model_2 = AutoModelForSequenceClassification.from_pretrained("./output/refinenet", num_labels=3)
evaluator = Trainer(
    model=model_2,
    args=training_args,
    eval_dataset=test_dataset,
    compute_metrics=compute_metrics,
)
evaluator.evaluate(eval_dataset=test_dataset) 







