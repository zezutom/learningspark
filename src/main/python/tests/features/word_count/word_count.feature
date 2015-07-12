Feature: Apache Spark makes it possible to easily count words in text files

  Background:
    Given I am using Apache Spark
    And I am using the WordCount

  Scenario: Calculate words in an empty set
    Given I provide no content
    Then I should see no result

  Scenario: Calculate words in Robert Frost's famous quote
    Given I provide Robert Frost's famous quote
    Then I should see the expected word count