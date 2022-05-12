from transformers import AutoModelForTokenClassification, AutoTokenizer, pipeline
import pandas as pd
import spacy
from spacy.tokens import Span
from spacy.language import Language
from relation_extraction import get_relations

class RelationsPipeline:

    def __init__(self, nlp = spacy.load('en_core_web_sm',exclude='ner')):
        self.nlp = nlp
        self.initialize_spacy_pipeline()
        self.initialize_ner_model()
        self.ner_predictions = None

    def initialize_spacy_pipeline(self):
        """
        Add custom pipeline components for linguistic parsing to the Spacy pipeline
        """
        self.nlp.add_pipe('clausal_modifiers')
        self.nlp.add_pipe('PP_modifiers')

    def initialize_ner_model(self):
        """
        Load Transformer model for NER and its corresponding tokenizer
        """
        self.model = AutoModelForTokenClassification.from_pretrained('./Models/spaceroberta_CR')
        self.tokenizer = AutoTokenizer.from_pretrained('./Models/spaceroberta_CR', add_prefix_space=True)

    @Language.component('clausal_modifiers')
    def clausal_modifiers(doc):
        """
        Custom pipeline component for assigning Spacy spans for clausal modifiers (relative, adverbial, adjectival)

        :param doc: Spacy Doc object
        :return doc: Spacy Doc object
        """
        clausal_mods = []
        for token in doc:
            if token.dep_ in ['relcl', 'advcl', 'acl']: #Dependency tags for relative, adverbial, and adjectival clause modifiers
              clause_start = token.left_edge.i
              clause_end = token.right_edge.i + 1
              clausal_mods.append(Span(doc, clause_start, clause_end))
        doc.spans['clausal_mods'] = clausal_mods
        return doc


    @Language.component('PP_modifiers')
    def PP_modifiers(doc):
        """
        Custom pipeline component for assigning Spacy spans for prepositional phrase modifiers that are not part of other clausal modifiers.
        Can be multiple PP modifiers nested in one.

        :param doc: Spacy Doc object
        :return doc: Spacy Doc object
        """
        PP_mods = []
        clausal_mod_indices = [tok.i for mod in doc.spans['clausal_mods']
                             for tok in mod] #Token indices for clausal modifier spans
        for token in doc:
            if token.dep_ == 'prep': #Check if token is a preposition
                PP_indices = [tok.i for tok in list(token.subtree) if tok.i not in clausal_mod_indices] #Retain indices for PP modifier only if they're not part of a clausal modifier
                if PP_indices:
                    PP_mods.append(Span(doc, PP_indices[0], PP_indices[-1] + 1)) #Append PP modifier as Span object

        doc.spans['PP_mods'] = PP_mods
        return doc

    def get_ner_predictions(self, sentences):
        """
        Run sentences through Transformer inference pipeline to get entity predictions.

        :param sentences: list of sentences
        :return: none
        """

        inference_pipeline = pipeline(task='token-classification',model=self.model,tokenizer=self.tokenizer, aggregation_strategy='simple')
        predictions = inference_pipeline(sentences)

        self.ner_predictions = predictions

    def align_with_spacy(self, doc, prediction):
        """
        The Transformer inference pipeline specifies the start and stop indices of the predicted entities within the sentence string.
        We need to align these character indices with token indices obtained using Spacy's tokenizer in order to assign the entities to
        Spacy documents.

        :param doc: Spacy Doc object from a single sentence
        :param prediction: prediction output of a single sentence from Transformer inference pipeline
        :return doc: Spacy Doc object with the assigned entities
        """
        tokenized_predictions = []
        #Iterate over each predicted entity in the sentence
        for pred in prediction:
            entity = pred['word'].strip()
            tokenized_entity = [word.text for word in self.nlp.tokenizer(entity)] #Tokenize entity using Spacy's tokenizer
            label = pred['entity_group']
            start = pred['start'] #Starting character index of entity
            tokenized_predictions.append((tokenized_entity, label, start))

        spans = []
        for entity, label, start in tokenized_predictions:
            for word in doc:
                #Check if first token of entity and character index lines up
                if (entity[0] == word.text) and (start == word.idx):
                    spans.append(Span(doc, word.i, word.i + len(entity), label)) #Append span of tokens pertaining to the entity

        doc.ents = spans #Assign spans as entities to Spacy document
        return doc

    def export_relations(self, input_data, output_file):
        """
        Extract relationships between entities and export into a CSV file.

        :param input_data: input CSV file of sentences with header 'inputs'
        :param output_file: output CSV file for the extracted relations
        :return: none
        """
        df = pd.read_csv(input_data)
        sentences = list(df['inputs'].values)

        self.get_ner_predictions(sentences)
        docs = self.nlp.pipe(sentences)
        aligned_docs = [self.align_with_spacy(doc,pred) for doc,pred in zip(docs,self.ner_predictions)]
        df = get_relations(aligned_docs)
        df.to_csv(output_file)





