from transformers import TrainingArguments, AutoModelForSequenceClassification, Trainer, AutoTokenizer, AutoConfig, AutoModel
import polars as pl
from torch import nn


## define model path
MODELS = {
        'Bio_ClinicalBERT' : 'emilyalsentzer/Bio_ClinicalBERT', ## used by BERTMAP, may be better for clinical use cases.
        'PubMedBERT': 'microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext', ## PubMedBERT, uses PubMed so may be good for research terms
        'SapBERT': 'cambridgeltl/SapBERT-from-PubMedBERT-fulltext', ## SapBert trained with UMLS as KG
        }
model_name = 'SapBERT'
model_path = MODELS[model_name]
## load model and tokenizer
config = AutoConfig.from_pretrained(model_path)
model = AutoModel.from_config(config)
tokenizer = AutoTokenizer.from_pretrained(model_path)

## add in a linear model for classification 
classifier = nn.Linear(in_features=768, out_features=3, bias = True)
dropout = nn.Dropout(p = 0.5) ## add drop out layer
## load in some example data 
data_path = 'generated_maps.tsv'
df = pl.read_csv(data_path, separator='\t')
for row in df.iter_rows(named = True):
    break

## try to tokenize it 
label = row.pop('class')
txt_rep = f"{row['source prefix']} | {row['source name']} [SEP] {row['target prefix']} | {row['target name']}"
tokenized_input = tokenizer(
    txt_rep,
    padding='max_length',
    truncation=True,
    max_length=256,  # adjust as needed
    return_tensors='pt'
)
## pass to bert model
res = model(**tokenized_input)
pooled_res = res.pooler_output ## take pooled output could also use the last hidden state
## drop out layer
dropout_res = dropout(pooled_res)
feature_res = classifier(dropout_res)
output = nn.functional.softmax(feature_res) ## could also use relu

