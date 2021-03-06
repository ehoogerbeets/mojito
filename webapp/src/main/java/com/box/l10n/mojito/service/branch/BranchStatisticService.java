package com.box.l10n.mojito.service.branch;

import com.box.l10n.mojito.entity.Branch;
import com.box.l10n.mojito.entity.BranchNotification;
import com.box.l10n.mojito.entity.BranchStatistic;
import com.box.l10n.mojito.entity.BranchTextUnitStatistic;
import com.box.l10n.mojito.quartz.QuartzJobInfo;
import com.box.l10n.mojito.quartz.QuartzPollableTaskScheduler;
import com.box.l10n.mojito.service.assetExtraction.AssetTextUnitToTMTextUnitRepository;
import com.box.l10n.mojito.service.branch.notification.job.BranchNotificationJob;
import com.box.l10n.mojito.service.branch.notification.job.BranchNotificationJobInput;
import com.box.l10n.mojito.service.tm.TMTextUnitRepository;
import com.box.l10n.mojito.service.tm.search.TextUnitAndWordCount;
import com.box.l10n.mojito.service.tm.search.TextUnitDTO;
import com.box.l10n.mojito.service.tm.search.TextUnitSearcher;
import com.box.l10n.mojito.service.tm.search.TextUnitSearcherParameters;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.box.l10n.mojito.service.assetExtraction.AssetExtractionService.PRIMARY_BRANCH;
import static com.box.l10n.mojito.service.tm.search.StatusFilter.FOR_TRANSLATION;
import static org.slf4j.LoggerFactory.getLogger;

@Component
public class BranchStatisticService {

    /**
     * logger
     */
    static Logger logger = getLogger(BranchStatisticService.class);

    @Autowired
    BranchService branchService;

    @Autowired
    BranchRepository branchRepository;

    @Autowired
    BranchStatisticRepository branchStatisticRepository;

    @Autowired
    BranchTextUnitStatisticRepository branchTextUnitStatisticRepository;

    @Autowired
    QuartzPollableTaskScheduler quartzPollableTaskScheduler;

    @Autowired
    TextUnitSearcher textUnitSearcher;

    @Autowired
    TMTextUnitRepository tmTextUnitRepository;

    @Autowired
    AssetTextUnitToTMTextUnitRepository assetTextUnitToTMTextUnitRepository;

    /**
     * Compute statistics for all branches that are not deleted in a given repository.
     *
     * @param repositoryId
     */
    public void computeAndSaveBranchStatistics(Long repositoryId) {
        logger.debug("computeAndSaveBranchStatistics for repository: {}", repositoryId);
        List<Branch> branchesToCheck = getBranchesToProcess(repositoryId);
        for (Branch branch : branchesToCheck) {
            computeAndSaveBranchStatistics(branch);
            scheduleBranchNotification(branch);
        }
    }

    void scheduleBranchNotification(Branch branch) {
        BranchNotificationJobInput branchNotificationJobInput = new BranchNotificationJobInput();
        branchNotificationJobInput.setBranchId(branch.getId());

        QuartzJobInfo quartzJobInfo = QuartzJobInfo.newBuilder(BranchNotificationJob.class)
                .withUniqueId(String.valueOf(branch.getId()))
                .withInput(branchNotificationJobInput).build();

        quartzPollableTaskScheduler.scheduleJob(quartzJobInfo);
    }

    /**
     * Computes and save branch statistics
     *
     * @param branch
     */
    public void computeAndSaveBranchStatistics(Branch branch) {

        logger.debug("computeAndSaveBranchStatistics for branch: {} ({})", branch.getId(), branch.getName());

        BranchStatistic branchStatistic = branchStatisticRepository.findByBranch(branch);

        if (branchStatistic == null) {
            logger.debug("No branchStatistic, create it");
            branchStatistic = new BranchStatistic();
            branchStatistic.setBranch(branch);
            branchStatistic = branchStatisticRepository.save(branchStatistic);
        }

        Set<Long> tmTextUnitIdsToRemove = getTmTextUnitIdsOfBranchStatistic(branchStatistic);
        List<TextUnitDTO> branchTextUnits = getTextUnitDTOsForBranch(branch);

        long sumTotalCount = 0;
        long sumForTranslationCount = 0;

        for (TextUnitDTO textUnitDTO : branchTextUnits) {
            long tmTextUnitId = textUnitDTO.getTmTextUnitId();
            tmTextUnitIdsToRemove.remove(tmTextUnitId);

            logger.debug("Get BranchTextUnitStatistic for tmTextUnitId: {}", tmTextUnitId);
            BranchTextUnitStatistic branchTextUnitStatistic = branchTextUnitStatisticRepository.getByBranchStatisticIdAndTmTextUnitId(branchStatistic.getId(), tmTextUnitId);

            if (branchTextUnitStatistic == null) {
                logger.debug("BranchTextUnitStatistic entity doesn't exist, create it");
                branchTextUnitStatistic = new BranchTextUnitStatistic();
                branchTextUnitStatistic.setBranchStatistic(branchStatistic);
                branchTextUnitStatistic.setTmTextUnit(tmTextUnitRepository.getOne(tmTextUnitId));
            }

            long forTranslationCount = getForTranslationCount(tmTextUnitId);
            sumForTranslationCount += forTranslationCount;

            long totalCount = getTotalCount(tmTextUnitId);
            sumTotalCount += totalCount;

            logger.debug("Update counts, forTranslation: {}, total: {}", forTranslationCount, totalCount);
            branchTextUnitStatistic.setForTranslationCount(forTranslationCount);
            branchTextUnitStatistic.setTotalCount(totalCount);

            branchTextUnitStatisticRepository.save(branchTextUnitStatistic);
        }

        branchStatistic.setForTranslationCount(sumForTranslationCount);
        branchStatistic.setTotalCount(sumTotalCount);
        branchStatisticRepository.save(branchStatistic);

        logger.debug("Remove statistic for unused text units for branch: {} ({})", branch.getId(), branch.getName());
        int removedCount = branchTextUnitStatisticRepository.deleteByBranchStatisticBranchIdAndTmTextUnitIdIn(branch.getId(), tmTextUnitIdsToRemove);
        logger.debug("Removed statistic: {}", removedCount);
    }

    Set<Long> getTmTextUnitIdsOfBranchStatistic(BranchStatistic branchStatistic) {
        return branchStatistic.getBranchTextUnitStatistics().stream().
                map(branchTextUnitStatistic -> branchTextUnitStatistic.getTmTextUnit().getId()).
                collect(Collectors.toSet());
    }

    /**
     * The branches to process/compute statistics for are the ones that are not deleted AND are not the primary branch
     * (branch with null name is also excluded at the moment since this mean we're not really working with branches)
     *
     * @param repositoryId
     * @return
     */
    List<Branch> getBranchesToProcess(Long repositoryId) {
        List<Branch> branches = branchRepository.findByRepositoryIdAndDeletedFalseAndNameNotNullAndNameNot(repositoryId, PRIMARY_BRANCH);
        return branches;
    }

    /**
     * Gets the text units of the specified branch.
     *
     * @param branch
     * @return
     */
    public List<TextUnitDTO> getTextUnitDTOsForBranch(Branch branch) {
        logger.debug("Get text units for branch: {} ({})", branch.getId(), branch.getName());

        List<Long> branchTmTextUnitIds = assetTextUnitToTMTextUnitRepository.findByBranch(branch);

        List<TextUnitDTO> textUnitDTOS;

        if (branchTmTextUnitIds.isEmpty()) {
            textUnitDTOS = Collections.emptyList();
        } else {
            TextUnitSearcherParameters textUnitSearcherParameters = new TextUnitSearcherParameters();
            textUnitSearcherParameters.setRepositoryIds(branch.getRepository().getId());
            textUnitSearcherParameters.setTmTextUnitIds(branchTmTextUnitIds);
            textUnitSearcherParameters.setForRootLocale(true);

            textUnitDTOS = textUnitSearcher.search(textUnitSearcherParameters);
        }

        return textUnitDTOS;
    }

    long getForTranslationCount(long tmTextUnitId) {
        TextUnitSearcherParameters textUnitSearcherParameters = new TextUnitSearcherParameters();

        textUnitSearcherParameters.setTmTextUnitIds(tmTextUnitId);
        textUnitSearcherParameters.setStatusFilter(FOR_TRANSLATION);
        textUnitSearcherParameters.setToBeFullyTranslatedFilter(true);

        TextUnitAndWordCount textUnitAndWordCount = textUnitSearcher.countTextUnitAndWordCount(textUnitSearcherParameters);
        return textUnitAndWordCount.getTextUnitCount();
    }

    long getTotalCount(long tmTextUnitId) {
        TextUnitSearcherParameters textUnitSearcherParameters = new TextUnitSearcherParameters();
        textUnitSearcherParameters.setTmTextUnitIds(tmTextUnitId);
        TextUnitAndWordCount textUnitAndWordCount = textUnitSearcher.countTextUnitAndWordCount(textUnitSearcherParameters);
        return textUnitAndWordCount.getTextUnitCount();
    }

}
