package org.wso2.extension.siddhi.io.cdc.util;

import org.wso2.extension.siddhi.io.cdc.source.config.Database;
import org.wso2.extension.siddhi.io.cdc.source.config.QueryConfiguration;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.nodes.Node;
import java.util.HashMap;

/**
 * This class is used to read query-config.yaml
 */
public class MyYamlConstructor extends Constructor {

    private HashMap<String, Class<?>> classMap = new HashMap<String, Class<?>>();

    public MyYamlConstructor(Class<? extends Object> theRoot) {
        super(theRoot);
        classMap.put(QueryConfiguration.class.getName(), QueryConfiguration.class);
        classMap.put(Database.class.getName(), Database.class);
    }

    protected Class<?> getClassForNode(Node node) {
        String name = node.getTag().getClassName();
        Class<?> cl = classMap.get(name);
        if (cl == null) {
            throw new YAMLException("Class not found: " + name);
        } else {
            return cl;
        }
    }

}
