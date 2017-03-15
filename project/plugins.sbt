resolvers += Resolver.sonatypeRepo("releases")

libraryDependencies += "org.kohsuke" % "github-api" % "1.59"

addSbtPlugin("com.eed3si9n"    % "sbt-assembly"  % "0.14.3")
addSbtPlugin("com.eed3si9n"    % "sbt-buildinfo" % "0.6.1")
addSbtPlugin("io.get-coursier" % "sbt-coursier"  % "1.0.0-M15")
addSbtPlugin("org.scoverage"   % "sbt-scoverage" % "1.3.3")
addSbtPlugin("com.slamdata"    % "sbt-slamdata"  % "0.0.12")

disablePlugins(TravisCiPlugin)

scalacOptions ++= commonScalacOptions_2_10
