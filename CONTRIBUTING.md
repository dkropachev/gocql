# Contributing to the ScyllaDB GoCQL Driver

**TL;DR** - this manifesto sets out the bare minimum requirements for submitting a patch to gocql.

This guide outlines the process of landing patches in gocql and the general approach to maintaining the code base.

## Background

The goal of the gocql project is to provide a stable and robust CQL driver for Go. This is a community driven project that is coordinated by a small team of core developers.

## Minimum Requirement Checklist

The following is a check list of requirements that need to be satisfied in order for us to merge your patch:

* You should raise a pull request to scylladb/gocql on Github
* The pull request has a title that clearly summarizes the purpose of the patch
* The motivation behind the patch is clearly defined in the pull request summary
* You agree that your contribution is donated to the Apache Software Foundation (appropriate copyright is on all new files)
* The patch will merge cleanly
* The test coverage does not fall
* The merge commit passes the regression test suite on GitHub Actions
* `go fmt` has been applied to the submitted code
* A correctly formatted commit message, see below
* Notable changes (i.e. new features or changed behavior, bugfixes) are appropriately documented in CHANGELOG.md, functional changes also in godoc

If there are any requirements that can't be reasonably satisfied, please state this either on the pull request or as part of discussion on the mailing list. Where appropriate, the core team may apply discretion and make an exception to these requirements.

## Commit Message

The commit message format should be:

```
<short description>

<reason why the change is needed>

Patch by <authors>; reviewed by <Reviewers> for #####
```

Short description should:
* Be a short sentence.
* Start with a capital letter.
* Be written in the present tense.
* Summarize what is changed, not why it is changed.

Short description should not:
* End with a period.
* Use the word Fixes . Most commits fix something.

Long description / Reason:
* Should describe why the change is needed. What is fixed by the change? Why it it was broken before? What use case does the new feature solve?
* Consider adding details of other options that you considered when implementing the change and why you made the design decisions you made.

## Beyond The Checklist

In addition to stating the hard requirements, there are a bunch of things that we consider when assessing changes to the library. These soft requirements are helpful pointers of how to get a patch landed quicker and with less fuss.

### General QA Approach

The Scylla project needs to consider the ongoing maintainability of the library at all times. Patches that look like they will introduce maintenance issues for the team will not be accepted.

Your patch will get merged quicker if you have decent test cases that provide test coverage for the new behavior you wish to introduce.

Unit tests are good, integration tests are even better. An example of a unit test is `marshal_test.go` - this tests the serialization code in isolation. `cassandra_test.go` is an integration test suite that is executed against every version of Cassandra that gocql supports as part of the CI process on Travis.

That said, the point of writing tests is to provide a safety net to catch regressions, so there is no need to go overboard with tests. Remember that the more tests you write, the more code we will have to maintain. So there's a balance to strike there.

### Sign Off Procedure

Generally speaking, a pull request can get merged by any one of the project's committers. If your change is minor, chances are that one team member will just go ahead and merge it there and then. As stated earlier, suitable test coverage will increase the likelihood that a single reviewer will assess and merge your change. If your change has no test coverage, or looks like it may have wider implications for the health and stability of the library, the reviewer may elect to refer the change to another team member to achieve consensus before proceeding. Therefore, the tighter and cleaner your patch is, the quicker it will go through the review process.

### Supported Features

gocql is a low level wire driver for Cassandra CQL. By and large, we would like to keep the functional scope of the library as narrow as possible. We think that gocql should be tight and focused, and we will be naturally skeptical of things that could just as easily be implemented in a higher layer. Inevitably you will come across something that could be implemented in a higher layer, save for a minor change to the core API. In this instance, please strike up a conversation in the Cassandra community. Chances are we will understand what you are trying to achieve and will try to accommodate this in a maintainable way.
