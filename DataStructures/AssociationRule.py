class AssociationRule:
    lhs = []
    rhs = []
    support = 0
    confidence = 0

    def __init__(self, lhs, rhs, support, confidence):
        self.lhs = set(lhs)
        self.rhs = set(rhs)
        self.support = support
        self.confidence = confidence

    def __eq__(self, other):
        """Overrides the default implementation"""
        if isinstance(other, AssociationRule):
            return self.lhs == other.lhs and self.rhs == other.rhs \
                   and self.support == other.support and self.confidence == other.confidence
        return False

    def __hash__(self):
        return hash((tuple(self.lhs), tuple(self.rhs), self.support, self.confidence))

    def identifyRuleOnlyByLhsAndRhs(self):
        return hash((frozenset(self.lhs), frozenset(self.rhs)))

    def equalsIgnoringSupAndConf(self, other):
        return self.lhs == other.lhs and self.rhs == other.rhs

    def getItemset(self):
        return self.lhs.union(self.rhs)