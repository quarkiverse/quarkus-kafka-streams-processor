# Contributing guide

**Want to contribute? Great!**
We try to make it easy, and all contributions, even the smaller ones, are more than welcome. This includes bug reports,
fixes, documentation, examples... But first, read this page.

* [Reporting an issue](#reporting-an-issue)
* [Before you contribute](#before-you-contribute)
  + [Discuss non-trivial contribution ideas with committers](#discuss-non-trivial-contribution-ideas-with-committers)
  + [Code reviews](#code-reviews)
  + [Tests and documentation are not optional](#tests-and-documentation-are-not-optional)
  + [Continuous Integration](#continuous-integration)
  + [Copyright Header](#copyright-header)

## Reporting an issue

This project uses GitHub issues to manage the issues. Open an issue directly in GitHub.

If you believe you found a bug, and it's likely possible, please indicate a way to reproduce it, what you are seeing and
what you would expect to see. Don't forget to indicate your Quarkus, Java, Maven/Gradle and GraalVM version.


## Before you contribute

To contribute, use GitHub Pull Requests, from your **own** fork.

Also, make sure you have set up your Git authorship correctly:

```sh
git config --global user.name "Your Full Name"
git config --global user.email your.email@example.com
```
We use this information to acknowledge your contributions in release announcements.

### Discuss non-trivial contribution ideas with committers

If you're considering anything more than correcting a typo or fixing a minor bug, please discuss it by [creating an issue on our issue tracker](https://github.com/quarkiverse/quarkus-kafka-streams-processor/issues) before submitting a pull request. We're happy to provide guidance but please spend an hour or two researching the subject on your own including searching the forums for prior discussions.

### Code reviews

All submissions, need to be reviewed by at least one committer before
being merged. Please properly squash your pull requests before submitting them.

[GitHub Pull Request Review Process](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/reviewing-changes-in-pull-requests/about-pull-request-reviews)
is followed for every pull request.

### Tests and documentation are not optional

Don't forget to include tests in your pull requests. Also don't forget the documentation (reference documentation,
javadoc...).

### Copyright Header

When creating a new Java file, make sure to manually add your Copyright in the header, otherwise Amadeus' Copyright will be automatically added.
Ensure the following format to prevent it from being overridden:

````
/*-
 * #%L
 * Quarkus Kafka Streams Processor
 * %%
 * Copyright (C) [yyyy] [Your name]
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
````

### Continuous Integration

Because we are all humans, and to ensure Quarkus is stable for everyone, all changes must go through Quarkus continuous
integration. Quarkus CI is based on GitHub Actions, which means that everyone has the ability to automatically execute
CI in their forks as part of the process of making changes. We ask that all non-trivial changes go through this process,
so that the contributor gets immediate feedback, while at the same time keeping our CI fast and healthy for everyone.

[//]: # (TODO: Double check this part after uploading it to github )
The process requires only one additional step to enable Actions on your fork (clicking the green button in the actions
tab). [See the full video walkthrough](https://youtu.be/egqbx-Q-Cbg) for more details on how to do this.


