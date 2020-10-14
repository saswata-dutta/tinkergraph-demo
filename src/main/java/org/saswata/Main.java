package org.saswata;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.identity;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.repeat;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class Main {

  public static void main(String[] args) {
    GraphTraversalSource g = initGraph();
    seedData(g);
    //    g.V().forEachRemaining(System.out::println);
    //    g.E().forEachRemaining(System.out::println);

    String[] edgeFilter = {"has_customer", "has_sfid", "has_payer"};

    System.out.println("parents");
    System.out.println("a2" + findParents(g, "a2", edgeFilter));
    System.out.println("a6" + findParents(g, "a6", edgeFilter));

    System.out.println("a2" + findParents(g, "a2", new String[] {"has_payer"}));
    System.out.println("a2" + findParents(g, "a2", new String[] {"has_sfid"}));

    System.out.println("a1" + findParents(g, "a1", edgeFilter));
    System.out.println("c_x" + findParents(g, "c_x", edgeFilter));
    System.out.println("c_y" + findParents(g, "c_y", edgeFilter));
    System.out.println("dummy" + findParents(g, "dummy", edgeFilter));

    System.out.println("related");
    System.out.println("a1" + findRelated(g, "a1", edgeFilter, "account"));
    System.out.println("a2" + findRelated(g, "a2", edgeFilter, "account"));
    System.out.println("a3" + findRelated(g, "a3", edgeFilter, "account"));
    System.out.println("a4" + findRelated(g, "a4", edgeFilter, "account"));
    System.out.println("a5" + findRelated(g, "a5", edgeFilter, "account"));
    System.out.println("a6" + findRelated(g, "a6", edgeFilter, "account"));
    System.out.println("a7" + findRelated(g, "a7", edgeFilter, "account"));

    System.out.println("c_x" + findRelated(g, "c_x", edgeFilter, "account"));
    System.out.println("c_y" + findRelated(g, "c_y", edgeFilter, "account"));

    System.out.println("sf_a" + findRelated(g, "sf_a", edgeFilter, "account"));
    System.out.println("sf_b" + findRelated(g, "sf_b", edgeFilter, "account"));
    System.out.println("sf_c" + findRelated(g, "sf_c", edgeFilter, "account"));

    System.out.println("dummy" + findRelated(g, "dummy", edgeFilter, "account"));
  }

  static List<List<String>> findParents(GraphTraversalSource g, String start, String[] edgeFilter) {

    if (!g.V(start).hasNext()) {
      System.out.println("bad vertex");
      return null;
    }

    GraphTraversal<Vertex, Path> traversal =
        g.V(start)
            .repeat(out(edgeFilter).simplePath())
            .until(outE(edgeFilter).limit(1).count().is(0))
            .path()
            .by(T.id);

    return traversal.toStream().map(Main::getPathIds).collect(Collectors.toList());
  }

  private static List<String> getPathIds(Path path) {
    List<String> pathIds =
        path.stream()
            .map(it -> it.getValue0().toString())
            .collect(Collectors.toCollection(LinkedList::new));
    pathIds.remove(0);
    return pathIds;
  }

  static Set<String> findRelated(
      GraphTraversalSource g, String start, String[] edgeFilter, String vertexFilter) {

    if (!g.V(start).hasNext()) {
      System.out.println("bad vertex");
      return null;
    }

    return g.V(start)
        .choose(
            out(edgeFilter).count().is(P.gt(0)),
            repeat(out(edgeFilter).simplePath()).until(outE(edgeFilter).limit(1).count().is(0)),
            identity())
        .repeat(both(edgeFilter).dedup())
        .emit(hasLabel(vertexFilter))
        .id()
        .toStream()
        .map(Object::toString)
        .collect(Collectors.toSet());
  }

  static GraphTraversalSource initGraph() {
    Graph graph = TinkerGraph.open();
    return graph.traversal();
  }

  static void seedData(GraphTraversalSource g) {
    g.addV("account")
        .property(T.id, "a1")
        .as("a1")
        .addV("account")
        .property(T.id, "a2")
        .as("a2")
        .addV("account")
        .property(T.id, "a3")
        .as("a3")
        .addV("account")
        .property(T.id, "a4")
        .as("a4")
        .addV("account")
        .property(T.id, "a5")
        .as("a5")
        .addV("account")
        .property(T.id, "a6")
        .as("a6")
        .addV("account")
        .property(T.id, "a7")
        .as("a7")
        .addV("sfid")
        .property(T.id, "sf_a")
        .as("sf_a")
        .addV("sfid")
        .property(T.id, "sf_b")
        .as("sf_b")
        .addV("sfid")
        .property(T.id, "sf_c")
        .as("sf_c")
        .addV("customer")
        .property(T.id, "c_x")
        .as("c_x")
        .addV("customer")
        .property(T.id, "c_y")
        .as("c_y")
        .addE("has_payer")
        .from("a2")
        .to("a4")
        .addE("has_sfid")
        .from("a5")
        .to("sf_b")
        .addE("has_sfid")
        .from("a3")
        .to("sf_a")
        .addE("has_sfid")
        .from("a4")
        .to("sf_a")
        .addE("has_sfid")
        .from("a6")
        .to("sf_c")
        .addE("has_sfid")
        .from("a7")
        .to("sf_c")
        .addE("has_customer")
        .from("sf_b")
        .to("c_x")
        .addE("has_customer")
        .from("sf_a")
        .to("c_x")
        .addE("has_customer")
        .from("c_x")
        .to("c_y")
        .addE("has_customer")
        .from("a1")
        .to("c_y")
        .addE("has_customer")
        .from("a6")
        .to("c_y")
        .addE("has_customer")
        .from("sf_c")
        .to("c_y")
        .iterate();
  }
}
