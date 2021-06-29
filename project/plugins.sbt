addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.+")
addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.+")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.+")
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.+")
addSbtPlugin("com.47deg"  % "sbt-microsites" % "1.+")
addSbtPlugin("org.scalameta" % "sbt-mdoc" % "2.+" )
addSbtPlugin("io.github.davidgregory084" % "sbt-tpolecat" % "0.+")
addSbtPlugin("com.thesamet" % "sbt-protoc" % "1.+")
addSbtPlugin("me.limansky" % "sbt-git-flow-version" % "0.+")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.+"