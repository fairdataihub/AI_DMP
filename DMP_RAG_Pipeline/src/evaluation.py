import difflib
from rouge_score import rouge_scorer
from logger.custom_logger import GLOBAL_LOGGER as log


class DMPEvaluator:
    """Compares generated DMPs against gold standards."""

    def __init__(self):
        self.scorer = rouge_scorer.RougeScorer(['rouge1', 'rougeL'], use_stemmer=True)

    def compare(self, gen_text, gold_text):
        seq = difflib.SequenceMatcher(None, gen_text, gold_text)
        fuzzy = seq.ratio()
        rouge = self.scorer.score(gen_text, gold_text)
        scores = {
            "fuzzy": round(fuzzy, 3),
            "rouge1": round(rouge['rouge1'].fmeasure, 3),
            "rougeL": round(rouge['rougeL'].fmeasure, 3)
        }
        log.info("Compared DMP pair", scores=scores)
        return scores

    def summarize(self, comparisons):
        if not comparisons:
            return {}
        keys = comparisons[0].keys()
        summary = {k: round(sum(d[k] for d in comparisons) / len(comparisons), 3) for k in keys}
        log.info("📊 Evaluation Summary", summary=summary)
        return summary
