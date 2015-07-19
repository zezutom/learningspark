Feature: Accumulator variables make aggregations easier

  Background:
    Given I am using Apache Spark
    And I am using Accumulator

  Scenario: An empty set should yield zero
    Given I provide an empty set
    Then The resulting sum should be zero

  Scenario: A numeric sequence should be summed up
    Given I provide a sequence of 1 to 4
    Then The resulting sum should be ten

