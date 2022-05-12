from spacy.tokens import Span, Doc
from typing import List, Tuple
import pandas as pd

def get_subjects_objects(doc: Doc) -> Tuple[List[Doc], List[Doc]]:
    """
    Get subjects that are arguments of the predicate and all objects. Doc argument expected to be a single sentence.

    :param doc: SpaCy Doc object
    :return subjects: list of subjects tied to the sentence predicate
    :return object: list of objects
    """
    sent_span = doc[0:]
    subjects = []
    objects = []

    # Find all subject, object, and attribute (for is-a relations) noun chunks
    for chunk in doc.noun_chunks:

        # (noun chunk root is nominal subject) & (noun chunk contains entities) & (noun chunk root is the sentence root)
        if ('nsubj' in chunk.root.dep_) and (chunk.ents) and (chunk.root.head == sent_span.root):
            conjuncts = list(chunk.conjuncts)
            conjunct_spans = [Span(doc, conj.left_edge.i, conj.i + 1) for conj in conjuncts]
            subjects.extend([chunk] + conjunct_spans)

        # Noun chunk root is an object or attribute
        if chunk.root.dep_ in ['dobj', 'pobj', 'attr']:
            conjuncts = list(chunk.conjuncts)
            conjunct_spans = [Span(doc, conj.left_edge.i, conj.i + 1) for conj in conjuncts]
            objects.extend([chunk] + conjunct_spans)

    return subjects, objects


def get_predicate_triples(doc: Doc) -> List[Tuple[Doc, Doc, Doc, Doc]]:
    """
    Extract SVO (subject, verb, object) triple from a sentence for verbs in the predicate. Doc argument expected to be a single
    sentence. Only subjects that are named entities are considered.

    :param doc: SpaCy Doc object
    :return triples: SVO triples with adverbial prepositional phrase modifier if it exists
    """
    sent_span = doc[0:]
    predicate = [sent_span.root] + list(sent_span.conjuncts)
    subjects, objects = get_subjects_objects(doc)

    # Map verbs in predicate to corresponding objects (direct or prepositional)
    verb_object_mapping = []
    object_roots = [obj.root for obj in objects]

    # Iterate through each verb that makes up the predicate
    for verb in predicate:

        # Check right children of the verbs
        for right_child in verb.rights:

            # Direct objects or attributes
            if right_child.dep_ in ['dobj', 'attr']:
                PP_mod = [mod for mod in doc.spans['PP_mods'] if mod.root.head == right_child]
                try:
                    index = object_roots.index(right_child)
                    if PP_mod:
                        verb_object_mapping.append((verb.lemma_, objects[index], PP_mod[0]))
                    else:
                        verb_object_mapping.append((verb.lemma_, objects[index], None))
                except:
                    pass

                # Conjunctions
                if right_child.conjuncts:
                    for conj in right_child.conjuncts:
                        PP_mod_conj = [mod for mod in doc.spans['PP_mods'] if mod.root.head == conj]
                        try:
                            index = object_roots.index(conj)
                            if PP_mod:
                                verb_object_mapping.append((verb.lemma_, objects[index], PP_mod[0]))
                            elif PP_mod_conj:
                                verb_object_mapping.append((verb.lemma_, objects[index], PP_mod_conj[0]))
                            else:
                                verb_object_mapping.append((verb.lemma_, objects[index], None))
                        except:
                            pass

            # Prepositions
            if right_child.dep_ == 'prep':

                prep_verb = ' '.join([verb.lemma_, right_child.text])  # Prepositional verb

                # Iterate over all children of the preposition
                for prep_child in right_child.children:

                    # Find object of the preposition
                    if prep_child.dep_ == 'pobj':
                        PP_mod = [mod for mod in doc.spans['PP_mods'] if mod.root.head == prep_child]
                        try:
                            index = object_roots.index(prep_child)
                            if PP_mod:
                                verb_object_mapping.append((prep_verb, objects[index], PP_mod[0]))
                            else:
                                verb_object_mapping.append((prep_verb, objects[index], None))
                        except:
                            pass

                        # Conjunctions
                        if prep_child.conjuncts:
                            for conj in prep_child.conjuncts:
                                PP_mod_conj = [mod for mod in doc.spans['PP_mods'] if mod.root.head == conj]
                                try:
                                    index = object_roots.index(conj)
                                    if PP_mod:
                                        verb_object_mapping.append((prep_verb, objects[index], PP_mod[0]))
                                    elif PP_mod_conj:
                                        verb_object_mapping.append((verb.lemma_, objects[index], PP_mod_conj[0]))
                                    else:
                                        verb_object_mapping.append((prep_verb, objects[index], None))
                                except:
                                    pass

    triples = [(subj, verb, obj, mod) for subj in subjects
               for verb, obj, mod in verb_object_mapping]
    return triples


def get_relcl_triples(doc: Doc) -> List[Tuple[Doc, Doc, Doc, Doc]]:
    """
    Extract SVO (subject, verb, object) triples for subjects in a relative clause modifier. Only subjects that
    are named entities are considered.

    :param doc: SpaCy Doc object
    :return triples: SVO triples with associated adverbial prepositional phrase modifier if it exists
    """
    relcl_object_mapping = []
    _, objects = get_subjects_objects(doc)
    object_roots = [obj.root for obj in objects]
    triples = []

    # Iterate through each token
    for token in doc:

        # Find relative clause modifiers
        if token.dep_ == 'relcl':

            # Check if it's an object being modified
            if token.head.dep_ in ['dobj', 'pobj']:
                try:
                    index = object_roots.index(token.head)
                    relcl_subj = objects[index] #assign an object noun chunk as the relative clause subject
                except:
                    continue

                # Check if relative clause subject contains entities
                if relcl_subj.ents:
                    relcl_conjuncts = [token] + list(token.conjuncts)

                    # Iterate over verbs in the relative clause
                    for verb in relcl_conjuncts:
                        # Iterate over right children of the verb
                        for right_child in verb.rights:

                            # Direct object or attribute
                            if right_child.dep_ in ['dobj', 'attr']:
                                PP_mod = [mod for mod in doc.spans['PP_mods'] if mod.root.head == right_child]
                                try:
                                    index = object_roots.index(right_child)
                                    if PP_mod:
                                        relcl_object_mapping.append((verb.lemma_, objects[index], PP_mod[0]))
                                    else:
                                        relcl_object_mapping.append((verb.lemma_, objects[index], None))
                                except:
                                    pass

                            # Conjunctions
                            if right_child.conjuncts:

                                for conj in right_child.conjuncts:
                                    PP_mod_conj = [mod for mod in doc.spans['PP_mods'] if mod.root.head == conj]
                                    try:
                                        index = object_roots.index(conj)
                                        if PP_mod:
                                            relcl_object_mapping.append((verb.lemma_, objects[index], PP_mod[0]))
                                        elif PP_mod_conj:
                                            relcl_object_mapping.append((verb.lemma_, objects[index], PP_mod_conj[0]))
                                        else:
                                            relcl_object_mapping.append((verb.lemma_, objects[index], None))
                                    except:
                                        pass

                            # Prepositions
                            if right_child.dep_ == 'prep':

                                prep_verb = ' '.join([verb.lemma_, right_child.text])  # Get prepositional verb

                                # Iterate through all children of the preposition
                                for prep_child in right_child.children:

                                    # Find object of the preposition
                                    if prep_child.dep_ == 'pobj':
                                        PP_mod = [mod for mod in doc.spans['PP_mods'] if mod.root.head == prep_child]
                                        try:
                                            index = object_roots.index(prep_child)
                                            if PP_mod:
                                                relcl_object_mapping.append((prep_verb, objects[index], PP_mod[0]))
                                            else:
                                                relcl_object_mapping.append((prep_verb, objects[index], None))
                                        except:
                                            pass

                                        # Conjunctions
                                        if prep_child.conjuncts:

                                            for conj in prep_child.conjuncts:
                                                PP_mod_conj = [mod for mod in doc.spans['PP_mods'] if
                                                               mod.root.head == conj]
                                                try:
                                                    index = object_roots.index(conj)
                                                    if PP_mod:
                                                        relcl_object_mapping.append(
                                                            (prep_verb, objects[index], PP_mod[0]))
                                                    elif PP_mod_conj:
                                                        relcl_object_mapping.append(
                                                            (prep_verb, objects[index], PP_mod_conj[0]))
                                                    else:
                                                        relcl_object_mapping.append((prep_verb, objects[index], None))
                                                except:
                                                    pass

                    triples.extend([(relcl_subj, verb, obj, mod) for verb, obj, mod in relcl_object_mapping])

    return triples


def get_all_triples(doc: Doc) -> List[Tuple[Doc, Doc, Doc, Doc]]:
    """
    If the sentence root is a VERB or AUX, extract the predicate triples and relative clause triples.

    :param doc: SpaCy Doc object
    :return all_triples: combined list of predicate and relative clause triples
    """
    sent_span = doc[0:]

    if (sent_span.root.pos_ == 'VERB') or (sent_span.root.pos_ == 'AUX'):
        pred_triples = get_predicate_triples(doc) # predicate triples
        relcl_triples = get_relcl_triples(doc) # relative clause triples
        all_triples = pred_triples + relcl_triples # combine

        return all_triples


def get_relations(docs: List[Doc]) -> pd.DataFrame:
    """
    Output a dataframe listing extracted relations (verbs) between a source (subject) and a target (object) along with the modifier of the
    relation. 'Source Root' and 'Target Root' columns are the roots of the source and target phrases according to their dependency tree.
    'Modifier' column are the adverbial prepositional phrase modifiers for each relation if it exists.

    :param docs: list of SpaCy Doc objects
    :return relations: dataframe of relations
    """
    columns = ['Sentence', 'Source', 'Source Root', 'Relation', 'Target', 'Target Root', 'Modifier']
    relations = pd.DataFrame(columns=columns)

    for doc in docs:
        triples = get_all_triples(doc)
        if triples:
            triples = [(doc.text,
                        subj.text,
                        subj.root.lemma_,
                        verb,
                        obj.text,
                        obj.root.lemma_,
                        mod)
                       for subj, verb, obj, mod in triples]

        triples = pd.DataFrame(triples, columns=columns)
        relations = pd.concat([relations, triples])

    relations = relations.reset_index(drop=True)
    return relations