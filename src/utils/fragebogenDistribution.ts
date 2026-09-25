export type FragebogenDistributionScore = {
  yes: number;
  total: number;
  percentage: number | null;
};

type DistributionAnswer = {
  question_id: string;
  answer_boolean?: boolean | null;
};

export const toFragebogenDistributionScore = (
  value?: { yes: number; total: number }
): FragebogenDistributionScore => {
  const yes = value?.yes || 0;
  const total = value?.total || 0;
  return {
    yes,
    total,
    percentage: total > 0 ? Math.round((yes / total) * 100) : null
  };
};

export const scoreFragebogenDistributionAnswers = (
  answers: DistributionAnswer[],
  distributionQuestionIds: Set<string>
): FragebogenDistributionScore => {
  const counts = answers.reduce(
    (result, answer) => {
      if (
        !distributionQuestionIds.has(answer.question_id)
        || typeof answer.answer_boolean !== 'boolean'
      ) {
        return result;
      }

      result.total += 1;
      if (answer.answer_boolean) result.yes += 1;
      return result;
    },
    { yes: 0, total: 0 }
  );

  return toFragebogenDistributionScore(counts);
};
