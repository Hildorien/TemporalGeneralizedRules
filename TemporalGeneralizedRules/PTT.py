from .HTAR_utility import getPeriodsIncluded


class PTT:  # Periodical Total Transaction
    # A list of lists. Outer list is l-levels (Hardcoded in 3 elements). Inner lists are the amount of transactions contained in each time period contained in each level.
    # They are initialized in 0. So if your HTG is [4,2,5] the ptt created will be [0,0,0,0]

    # PTT is only saving data for 0-level time granules

    def __init__(self):
        hardcoded_htg = [24, 12, 4, 1]
        self.ptt = []
        for x in range(hardcoded_htg[0]):
            self.ptt.append({'itemsSet': set(), 'totalTransactions': 0, 'FirstAndLastTID': None})

    def addMultiplePeriods(self, periods):
        for period in periods:
            self.addItemPeriodToPtt(period)

    def getTotalPTTSumWithinPeriodsInLevel0(self, boundaries):
        return sum(list(map(lambda pi: self.getPTTValueFromLeafLevelGranule(pi)['totalTransactions'],
                            range(boundaries[0], boundaries[1] + 1))))

    def getEveryItemInPTTInPG(self, l_level, period):
        level_0_periods_included = getPeriodsIncluded(l_level, period)
        finalItems = set()
        for pj in range(level_0_periods_included[0], level_0_periods_included[1]+1):
            leafSet = self.getPTTValueFromLeafLevelGranule(pj)['itemsSet']
            finalItems.update(leafSet)
        return finalItems


    def addItemPeriodToPtt(self, pi, tid=0, items=set()):
        # pi is the leaf-granule where to add the items
        if self.checkIfLlevelAndPeriodAreValid(pi):
            self.ptt[pi - 1]['itemsSet'].update(items)
            self.ptt[pi - 1]['totalTransactions'] += 1
            if self.ptt[pi - 1]['FirstAndLastTID'] is None:
                self.ptt[pi - 1]['FirstAndLastTID'] = [tid, tid]
            else:
                self.ptt[pi - 1]['FirstAndLastTID'][1] = tid
        else:
            print('ERROR AL AGREGAR ELEMENTO AL PTT. PERIODOS NO COMPATIBLE CON HTG')

    def getPTTValueFromLeafLevelGranule(self, pi):
        if self.checkIfLlevelAndPeriodAreValid(pi - 1):
            return self.ptt[pi - 1]
        else:
            print('ERROR AL OBTENER ELEMENTO EN LA PTT. EL NIVEL O PERIODO ASOCIADO NO EXISTE')

    def getPTTValueFromNonLeafLevelGranule(self, level, pi):
        level_0_periods_included = getPeriodsIncluded(level, pi)
        return self.getTotalPTTSumWithinPeriodsInLevel0(level_0_periods_included)

    def getPTTPeriodTIDBoundaryTuples(self):
        return list(map(lambda x: x['FirstAndLastTID'], self.ptt))

    def checkIfLlevelAndPeriodAreValid(self, period):
        return len(self.ptt) >= period
