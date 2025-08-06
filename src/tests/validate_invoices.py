from datetime import datetime

def validate_cvs_quarterly_results(summary_results, transaction_results):

    summary_totals = summary_results.sum(axis=0)

    fill_minimums = transaction_results[transaction_results['reversal indicator'] == 'B1'].min(
        axis=0)

    assert summary_totals["claim count"] == \
        len(transaction_results[transaction_results['reversal indicator'] == 'B1'].index) \
        - len(transaction_results[transaction_results['reversal indicator'] == 'B2'].index), \
        "Total claims in summary do not match fills minus reversals.\n" + \
        "\tTotal claims count: " + str(summary_totals["claim count"]) + "\n" + \
        "\tTotal fills: " + str(len(transaction_results[transaction_results['reversal indicator'] == 'B1'].index)) + "\n" + \
        "\tTotal reversals: " + str(len(transaction_results[transaction_results['reversal indicator'] == 'B2'].index)) + "\n" +  \
        "\tCalculated fill count: " + str(len(transaction_results[transaction_results['reversal indicator'] == 'B1'].index)
                                          - len(transaction_results[transaction_results['reversal indicator'] == 'B2'].index))

    assert datetime.strptime(fill_minimums["fill date"], '%Y-%m-%d') \
        >= datetime.strptime(summary_results.iloc[0]["period_start"], '%m/%d/%Y'), \
        "All fills do not start after the invoice start date. \n" \
        "\tEarliest transaction: " + str(fill_minimums["fill date"]) + "\n" + \
        "\tSummary start date: " + str(summary_results.iloc[0]["period_start"])

    ingredient_total = summary_results["total ingredient cost"].sum(axis=0)
    calculated_ingredient_cost = transaction_results["ingredient cost paid"].sum(
        axis=0)
    assert abs(ingredient_total - calculated_ingredient_cost) < 0.01, \
        "Ingredient cost does not match transactions ingredient costs\n" \
        "\tTotal ingredient cost: " + str(ingredient_total) + "\n" + \
        "\tTotal transaction ingredient cost: " + \
        str(calculated_ingredient_cost)

    administration_fee_total = summary_results["total administration fee"].sum(
        axis=0)
    calculated_administration_fee = round(
        transaction_results["administration fee"].sum(axis=0), 2)
    assert abs(administration_fee_total - calculated_administration_fee) < 0.01, \
        "Admin fee does not match sum of fills and reversals\n" \
        "\tTotal admin fee: " + str(administration_fee_total) + "\n" + \
        "\tTotal transactions admin fee: " + \
        str(calculated_administration_fee)

    dollar_variance = summary_results["dollar variance"].sum(axis=0)
    calculated_dollar_variance = round(
        transaction_results["dollar variance"].sum(axis=0), 2)
    assert abs(dollar_variance - calculated_dollar_variance) < 150, \
        "Variance does not match sum of fills and reversals\n" \
        "\tTotal variance: " + str(dollar_variance) + "\n" + \
        "\tTotal transactions variance: " + str(calculated_dollar_variance)
