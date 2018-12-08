const React = require('react');

const CompLibrary = require('../../core/CompLibrary.js');

const MarkdownBlock = CompLibrary.MarkdownBlock;
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

class HomeSplash extends React.Component {
  render() {
    const {siteConfig, language = ''} = this.props;
    const {baseUrl, docsUrl} = siteConfig;
    const docsPart = `${docsUrl ? `${docsUrl}/` : ''}`;
    const langPart = `${language ? `${language}/` : ''}`;
    const docUrl = doc => `${baseUrl}${docsPart}${langPart}${doc}`;

    const SplashContainer = props => (
      <div className="homeContainer">
        <div className="homeSplashFade">
          <div className="wrapper homeWrapper">{props.children}</div>
        </div>
      </div>
    );

    const ProjectTitle = () => (
      <h2 className="projectTitle">
        <span>
          <img className="projectTitleLogo" src={siteConfig.titleIcon} />
          {siteConfig.title}
        </span>
        <small>{siteConfig.tagline}</small>
      </h2>
    );

    const PromoSection = props => (
      <div className="section promoSection">
        <div className="promoRow">
          <div className="pluginRowBlock">{props.children}</div>
        </div>
      </div>
    );

    const Button = props => (
      <div className="pluginWrapper buttonWrapper">
        <a className="button" href={props.href} target={props.target}>
          {props.children}
        </a>
      </div>
    );

    return (
      <SplashContainer>
        <div className="inner">
          <ProjectTitle siteConfig={siteConfig} />
          <PromoSection>
            <Button href={docUrl("overview", language)}>Documentation</Button>
            <Button href={siteConfig.repoUrl}>View on GitHub</Button>
          </PromoSection>
        </div>
      </SplashContainer>
    );
  }
}

class Index extends React.Component {
  render() {
    const {config: siteConfig, language = ''} = this.props;
    const {baseUrl, buildInfo} = siteConfig;
    const {latestVersion, latestMinorVersion} = buildInfo;

    const Block = props => (
      <Container
        padding={['bottom', 'top']}
        id={props.id}
        background={props.background}>
        <GridBlock
          align="center"
          contents={props.children}
          layout={props.layout}
        />
      </Container>
    );

    const index =
`[![Travis](https://img.shields.io/travis/ovotech/fs2-kafka/master.svg)](https://travis-ci.org/ovotech/fs2-kafka) [![Codecov](https://img.shields.io/codecov/c/github/ovotech/fs2-kafka.svg)](https://codecov.io/gh/ovotech/fs2-kafka) [![Version](https://img.shields.io/badge/version-v${latestVersion}-orange.svg)](https://index.scala-lang.org/ovotech/fs2-kafka)

Functional streams for Kafka with [FS2][fs2] and the official Kafka Java client.  
Project under active development. Feedback and contributions welcome.

### Getting Started
To get started with [sbt][sbt], simply add the following line to your \`build.sbt\` file.

\`\`\`scala
libraryDependencies += "com.ovoenergy" %% "fs2-kafka" % "${latestVersion}"
\`\`\`

The library is published for Scala 2.11 and 2.12.

Backwards binary-compatibility for the library is guaranteed between patch versions.  
For example, \`${latestMinorVersion}.x\` is backwards binary-compatible with \`${latestMinorVersion}.y\` for any \`x > y\`.

[fs2]: https://fs2.io
[sbt]: https://scala-sbt.org
`.trim()

    return (
      <div>
        <HomeSplash siteConfig={siteConfig} language={language} />
        <div className="mainContainer">
          <div className="index">
            <MarkdownBlock>
              {index}
            </MarkdownBlock>
          </div>
        </div>
      </div>
    );
  }
}

module.exports = Index;
