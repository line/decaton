# How to contribute to Decaton

First of all, thank you so much for taking your time to contribute! Decaton is not very different from any other open source projects. It will be fantastic if you help us by doing any of the following:

- File an issue in [the issue tracker](https://github.com/line/decaton/issues) to report bugs and propose new features and improvements.
- Ask a question using [the issue tracker](https://github.com/line/decaton/issues).
- Contribute your work by sending [a pull request](https://github.com/line/decaton/pulls).

## Contributor license agreement

If you are sending a pull request and it's a non-trivial change beyond fixing typos, please make sure to sign the [ICLA (Individual Contributor License Agreement)](https://cla-assistant.io/line/decaton). Please [contact us](mailto:dl_oss_dev@linecorp.com) if you need the CCLA (Corporate Contributor License Agreement).

## Code of conduct

We expect contributors to follow [our code of conduct](CODE_OF_CONDUCT.md).

## Setting up your IDE

You can import this project into your IDE ([IntelliJ IDEA](https://www.jetbrains.com/idea/) or [Eclipse](https://www.eclipse.org/)) as a Gradle project.

- IntelliJ IDEA - See [Importing Project from Gradle Model](https://www.jetbrains.com/help/idea/gradle.html#gradle_import_project_start)
- Eclipse - Use [Buildship Gradle Integration](https://marketplace.eclipse.org/content/buildship-gradle-integration)

After importing the project, import the IDE settings as well.

### IntelliJ IDEA

- [`settings.jar`](https://raw.githubusercontent.com/line/armeria/master/settings/intellij_idea/settings.jar) -
  See [Import settings from a ZIP archive](https://www.jetbrains.com/help/idea/sharing-your-ide-settings.html#7a4f08b8).
- Make sure to use 'LINE OSS' code style and inspection profile.
  - Go to `Preferences` > `Editors` > `Code Style` and set `Scheme` option to `LINE OSS`.
  - Go to `Preferences` > `Editors` > `Inspections` and set `Profile` option to `LINE OSS`.

### Eclipse

- [`formatter.xml`](https://raw.githubusercontent.com/line/armeria/master/settings/eclipse/formatter.xml) -
  See [Code Formatter Preferences](https://help.eclipse.org/neon/index.jsp?topic=%2Forg.eclipse.jdt.doc.user%2Freference%2Fpreferences%2Fjava%2Fcodestyle%2Fref-preferences-formatter.htm).
- [`formatter.importorder`](https://raw.githubusercontent.com/line/armeria/master/settings/eclipse/formatter.importorder) -
  See [Organize Imports Preferences](https://help.eclipse.org/neon/index.jsp?topic=%2Forg.eclipse.jdt.doc.user%2Freference%2Fpreferences%2Fjava%2Fcodestyle%2Fref-preferences-organize-imports.htm).
- [`cleanup.xml`](https://raw.githubusercontent.com/line/armeria/master/settings/eclipse/cleanup.xml) -
  See [Clean Up Preferences](https://help.eclipse.org/neon/index.jsp?topic=%2Forg.eclipse.jdt.doc.user%2Freference%2Fpreferences%2Fjava%2Fcodestyle%2Fref-preferences-cleanup.htm).
- Configure [Java Save Actions Preferences](https://help.eclipse.org/neon/index.jsp?topic=%2Forg.eclipse.jdt.doc.user%2Freference%2Fpreferences%2Fjava%2Feditor%2Fref-preferences-save-actions.htm).
  <details><summary>Click here to see the screenshot.</summary>
    <img src="https://raw.githubusercontent.com/line/armeria/master/settings/eclipse/save_actions.png" alt="eclipse setting" />
  </details>

## Checklist for your pull request

Please use the following checklist to keep your contribution's quality high and
to save the reviewer's time.

### Configure your IDE

- Make sure you are using 'LINE OSS' code style and inspection profile.
- Evaluate all warnings emitted by the 'LINE OSS' inspection profile.
  - Try to fix them all and use the `@SuppressWarnings` annotation if it's a false positive.

### Always make the build pass

Make sure your change does not break the build.

- Run `./gradlew check` locally.
- It is likely that you'll encounter some Checkstyle or Javadoc errors.
  Please fix them because otherwise the build will be broken.

### Add copyright header

All source files must begin with the following copyright header:

```
Copyright $today.year LINE Corporation

LINE Corporation licenses this file to you under the Apache License,
version 2.0 (the "License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at:

  https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations
under the License.
```
