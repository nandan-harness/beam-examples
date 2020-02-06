

def currency_converter(rupees):
    dollar = rupees / 70
    return round(dollar, 2)


def run():
    rupees = [3500, 5000, 100, 1000]

    dollars = []
    for amount in rupees:
        converted_amount = currency_converter(amount)
        dollars.append(converted_amount)

    print(dollars)

if __name__ == "__main__":
    run()
