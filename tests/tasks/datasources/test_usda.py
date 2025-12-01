from tasks.datasources.usda import TransformHabit


def test_habits_tree():
    result = TransformHabit.transform_usda_habits(["Tree"])
    assert result == "tree"


def test_habits_shrub():
    result = TransformHabit.transform_usda_habits(["Shrub"])
    assert result == "shrub"


def test_habits_subshrub():
    result = TransformHabit.transform_usda_habits(["Subshrub"])
    assert result == "garden"


def test_habits_graminoid():
    result = TransformHabit.transform_usda_habits(["Graminoid"])
    assert result == "grass"


def test_habits_forb_herb():
    result = TransformHabit.transform_usda_habits(["Forb/herb"])
    assert result == "garden"


def test_habits_tree_shrub():
    result = TransformHabit.transform_usda_habits(["Tree", "Shrub"])
    assert result == "shrub"

    result = TransformHabit.transform_usda_habits(["Shrub", "Tree"])
    assert result == "shrub"


def test_habits_herb_subshrub():
    result = TransformHabit.transform_usda_habits(["Herb", "Subshrub"])
    assert result == "garden"

    result = TransformHabit.transform_usda_habits(["Subshrub", "Herb"])
    assert result == "garden"


def test_habits_tree_subshrub():
    result = TransformHabit.transform_usda_habits(["Tree", "Forb/herb"])
    assert result == "garden"

    result = TransformHabit.transform_usda_habits(["Forb/herb", "Tree"])
    assert result == "garden"


def test_habits_shrub_subshrub():
    result = TransformHabit.transform_usda_habits(["Shrub", "Subshrub"])
    assert result == "garden"

    result = TransformHabit.transform_usda_habits(["Subshrub", "Shrub"])
    assert result == "garden"
