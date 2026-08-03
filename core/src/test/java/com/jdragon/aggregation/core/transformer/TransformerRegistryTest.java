package com.jdragon.aggregation.core.transformer;

import com.jdragon.aggregation.core.plugin.PluginType;
import com.jdragon.aggregation.pluginloader.ConfigParser;
import com.jdragon.aggregation.pluginloader.JarLoaderCenter;
import com.jdragon.aggregation.pluginloader.LoadUtil;
import com.jdragon.aggregation.pluginloader.constant.SystemConstants;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeResolvers;
import com.jdragon.aggregation.pluginloader.runtime.PluginRuntimeSession;
import com.jdragon.aggregation.pluginloader.runtime.ResolvedPlugin;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TransformerRegistryTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final String originalPluginHome = SystemConstants.PLUGIN_HOME;
    private final String originalTransformerHome = TransformerRegistry.TRANSFORMER_HOME;

    @After
    public void tearDown() {
        SystemConstants.PLUGIN_HOME = originalPluginHome;
        TransformerRegistry.TRANSFORMER_HOME = originalTransformerHome;
        PluginRuntimeResolvers.reset();
        ConfigParser.clearCache();
        JarLoaderCenter.clearJarLoader();
    }

    @Test
    public void standaloneLookupReloadsTransformerAfterWeakEntryIsCleared() throws Exception {
        String transformerName = "standalone_reload";
        Path pluginHome = temporaryFolder.newFolder("aggregation", "plugin").toPath();
        Path pluginDirectory = pluginHome.resolve("transformer").resolve(transformerName);
        createTransformerPlugin(pluginDirectory, transformerName);
        SystemConstants.PLUGIN_HOME = pluginHome.toString();
        TransformerRegistry.TRANSFORMER_HOME = pluginHome.resolve("transformer").toString();

        TransformerRegistry.loadTransformer(transformerName);
        ResolvedPlugin resolved = LoadUtil.resolvePlugin(PluginType.TRANSFORMER, transformerName);
        WeakReference<TransformerInfo> cached = externalTransformers()
                .get(transformerName + "@" + resolved.getIdentity());
        assertNotNull(cached);
        cached.clear();

        TransformerInfo reloaded = TransformerRegistry.getTransformer(transformerName);

        assertNull(PluginRuntimeSession.current());
        assertNotNull(reloaded);
        assertEquals(transformerName, reloaded.getTransformer().getTransformerName());
        assertTrue(reloaded.getTransformer().getClass().getClassLoader() == reloaded.getClassLoader());
    }

    @SuppressWarnings("unchecked")
    private Map<String, WeakReference<TransformerInfo>> externalTransformers() throws Exception {
        Field field = TransformerRegistry.class.getDeclaredField("externalTransformers");
        field.setAccessible(true);
        return (Map<String, WeakReference<TransformerInfo>>) field.get(null);
    }

    private void createTransformerPlugin(Path pluginDirectory, String transformerName) throws Exception {
        Path sourceRoot = temporaryFolder.newFolder("transformer-source").toPath();
        Path classesRoot = temporaryFolder.newFolder("transformer-classes").toPath();
        Path source = sourceRoot.resolve("example").resolve("StandaloneTransformer.java");
        Files.createDirectories(source.getParent());
        Files.writeString(source,
                "package example;\n"
                        + "public final class StandaloneTransformer extends "
                        + "com.jdragon.aggregation.core.plugin.Transformer {\n"
                        + "  public com.jdragon.aggregation.commons.element.Record evaluate(\n"
                        + "      com.jdragon.aggregation.commons.element.Record record, Object... paras) {\n"
                        + "    return record;\n"
                        + "  }\n"
                        + "}\n",
                StandardCharsets.UTF_8);
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull("Tests require a JDK, not a JRE", compiler);
        int compileResult = compiler.run(null, null, null,
                "-classpath", System.getProperty("java.class.path"),
                "-d", classesRoot.toString(), source.toString());
        assertEquals("Synthetic transformer compilation failed", 0, compileResult);

        Files.createDirectories(pluginDirectory);
        try (JarOutputStream output = new JarOutputStream(
                Files.newOutputStream(pluginDirectory.resolve(transformerName + ".jar")))) {
            Path classFile = classesRoot.resolve("example").resolve("StandaloneTransformer.class");
            output.putNextEntry(new JarEntry("example/StandaloneTransformer.class"));
            Files.copy(classFile, output);
            output.closeEntry();
        }
        Files.writeString(pluginDirectory.resolve("transformer.json"),
                "{\"name\":\"" + transformerName
                        + "\",\"class\":\"example.StandaloneTransformer\","
                        + "\"description\":\"\",\"developer\":\"test\"}",
                StandardCharsets.UTF_8);
    }
}
